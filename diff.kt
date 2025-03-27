data class Change(
    val op: String,         // e.g., "replace", "add", "remove"
    val path: String,       // e.g., "/a/idA/b/idB/c"
    val value: Any? = null, // New value (optional)
    val itemIds: List<String> = emptyList() // IDs for array elements, e.g., ["idA", "idB"]
)

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.flipkart.zjsonpatch.JsonDiff

class DiffService(private val objectMapper: ObjectMapper) {

    fun computeDiff(oldNode: JsonNode, newNode: JsonNode): List<Change> {
        // Transform arrays to maps based on "id"
        val oldTransformed = transformToMap(oldNode)
        val newTransformed = transformToMap(newNode)

        // Compute the patch using zjsonpatch
        val patch = JsonDiff.asJson(oldTransformed, newTransformed)

        // Convert patch operations to Change objects
        return patch.map { operation ->
            val path = operation["path"].asText()
            val op = operation["op"].asText()
            val value = operation["value"]?.let { objectMapper.treeToValue(it, Any::class.java) }
            val itemIds = extractItemIds(path, oldNode)
            Change(op, path, value, itemIds)
        }
    }

    private fun transformToMap(node: JsonNode): JsonNode {
        if (node.isArray) {
            val mapNode = objectMapper.createObjectNode()
            node.elements().forEach { element ->
                val id = element.get("id")?.asText() ?: throw IllegalArgumentException("Missing id in array element")
                mapNode.set<JsonNode>(id, element)
            }
            return mapNode
        } else if (node.isObject) {
            val transformed = objectMapper.createObjectNode()
            node.fields().forEach { (key, value) ->
                transformed.set<JsonNode>(key, transformToMap(value))
            }
            return transformed
        }
        return node
    }

    private fun extractItemIds(path: String, node: JsonNode): List<String> {
        val parts = path.split("/").filter { it.isNotEmpty() }
        val itemIds = mutableListOf<String>()
        var current = node
        for (part in parts) {
            if (current.isArray && part.toIntOrNull() != null) {
                val index = part.toInt()
                val id = current.get(index)?.get("id")?.asText() ?: throw IllegalArgumentException("Missing id at $path")
                itemIds.add(id)
                current = current.get(index)
            } else if (current.isObject) {
                current = current.get(part) ?: throw IllegalArgumentException("Field $part not found")
            }
        }
        return itemIds
    }
}

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.flipkart.zjsonpatch.JsonPatch

fun applyChange(change: Change, currentNode: JsonNode, objectMapper: ObjectMapper): JsonNode {
    val pathParts = change.path.split("/").filter { it.isNotEmpty() }
    var adjustedPath = ""
    var current = currentNode
    var itemIdIndex = 0

    // Adjust the path based on current itemIds
    for (part in pathParts) {
        if (part.matches("\\d+".toRegex()) && current.isArray) {
            val id = change.itemIds.getOrNull(itemIdIndex++)
            val targetIndex = if (id != null) {
                current.elements().asSequence()
                    .indexOfFirst { it.get("id")?.asText() == id }
                    .takeIf { it >= 0 } ?: throw IllegalArgumentException("Object with id $id not found at /$adjustedPath")
            } else {
                part.toInt()
            }
            adjustedPath += "/$targetIndex"
            current = current.get(targetIndex) ?: throw IllegalArgumentException("Invalid index at /$adjustedPath")
        } else {
            adjustedPath += "/$part"
            current = current.get(part) ?: throw IllegalArgumentException("Field $part not found at /$adjustedPath")
        }
    }

    // Create the patch operation
    val patchOperation = objectMapper.createObjectNode().apply {
        put("op", change.op)
        put("path", adjustedPath)
        change.value?.let { set<JsonNode>("value", objectMapper.valueToTree(it)) }
    }
    val patchArray = objectMapper.createArrayNode().apply { add(patchOperation) }

    // Apply the patch using zjsonpatch
    return JsonPatch.apply(patchArray, currentNode)
}

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class DiffServiceTest {
    private val objectMapper = ObjectMapper()
    private val diffService = DiffService(objectMapper)

    @Test
    fun `diff and apply should work with zjsonpatch`() {
        val oldJson = """{"a": [{"id": "idA", "b": [{"id": "idB", "c": "value"}]}]}"""
        val newJson = """{"a": [{"id": "idA", "b": [{"id": "idB", "c": "newValue"}]}]}"""
        val currentJson = """{"a": [{"id": "idX", "b": []}, {"id": "idA", "b": [{"id": "idY", "c": "other"}, {"id": "idB", "c": "value"}]}]}"""

        val oldNode = objectMapper.readTree(oldJson)
        val newNode = objectMapper.readTree(newJson)
        val currentNode = objectMapper.readTree(currentJson)

        // Compute diff
        val changes = diffService.computeDiff(oldNode, newNode)
        assertEquals(1, changes.size)
        with(changes[0]) {
            assertEquals("replace", op)
            assertEquals("/a/idA/b/idB/c", path)
            assertEquals("newValue", value)
            assertEquals(listOf("idA", "idB"), itemIds)
        }

        // Apply change
        val updatedNode = applyChange(changes[0], currentNode, objectMapper)
        val expectedJson = """{"a": [{"id": "idX", "b": []}, {"id": "idA", "b": [{"id": "idY", "c": "other"}, {"id": "idB", "c": "newValue"}]}]}"""
        assertEquals(objectMapper.readTree(expectedJson), updatedNode)
    }
}
