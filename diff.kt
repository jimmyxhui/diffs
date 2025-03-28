import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.flipkart.zjsonpatch.JsonDiff

class DiffService(val objectMapper: ObjectMapper) {
    fun computeDiff(oldNode: JsonNode, newNode: JsonNode): List<Change> {
        val oldTransformed = transformToMap(oldNode)
        val newTransformed = transformToMap(newNode)
        val patch = JsonDiff.asJson(oldTransformed, newTransformed)
        return patch.map { operation ->
            val transformedPath = operation["path"].asText()
            val op = operation["op"].asText()
            val newValue = operation["value"]?.let { objectMapper.treeToValue(it, Any::class.java) }
            val originalPath = untransformPath(transformedPath, oldNode)
            val oldValue = getValueAtPath(originalPath, oldNode)
            Change(op, originalPath, oldValue, newValue)
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

    private fun untransformPath(transformedPath: String, originalNode: JsonNode): String {
        val parts = transformedPath.split("/").filter { it.isNotEmpty() }
        var current = originalNode
        var resultPath = ""
        for (part in parts) {
            if (current.isArray) {
                val id = part
                resultPath += "/$id"
                current = current.elements().asSequence()
                    .firstOrNull { it.get("id")?.asText() == id }
                    ?: throw IllegalArgumentException("ID $id not found in array at $resultPath")
            } else {
                resultPath += "/$part"
                current = current.get(part) ?: throw IllegalArgumentException("Field $part not found at $resultPath")
            }
        }
        return resultPath
    }

    private fun getValueAtPath(path: String, node: JsonNode): Any? {
        val parts = path.split("/").filter { it.isNotEmpty() }
        var current = node
        for (part in parts) {
            if (current.isArray) {
                val id = part
                current = current.elements().asSequence()
                    .firstOrNull { it.get("id")?.asText() == id }
                    ?: return null // Value didn’t exist in old node
            } else {
                current = current.get(part) ?: return null
            }
        }
        return if (current.isValueNode || current.isObject || current.isArray) {
            objectMapper.treeToValue(current, Any::class.java)
        } else null
    }
}

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode

fun applyChangeCustom(change: Change, currentNode: JsonNode, objectMapper: ObjectMapper): JsonNode {
    val pathParts = change.path.split("/").filter { it.isNotEmpty() }
    if (pathParts.isEmpty()) return currentNode

    val root = currentNode.deepCopy()
    var current = root
    var parent: JsonNode? = null
    var lastPart: String? = null

    for (i in pathParts.indices) {
        val part = pathParts[i]
        val isLastPart = i == pathParts.size - 1
        parent = current
        lastPart = part

        if (current.isArray) {
            val id = part
            val index = current.elements().asSequence()
                .mapIndexed { idx, node -> Pair(idx, node) }
                .firstOrNull { it.second.get("id")?.asText() == id }?.first
            if (index != null) {
                current = current.get(index)
            } else if (isLastPart && change.op == "add") {
                break
            } else {
                throw IllegalArgumentException("Object with id $id not found at ${pathParts.take(i + 1).joinToString("/", "/")}")
            }
        } else {
            current = current.get(part)
                ?: if (isLastPart && change.op == "add") break
                else throw IllegalArgumentException("Field $part not found at ${pathParts.take(i + 1).joinToString("/", "/")}")
        }
    }

    when (change.op) {
        "remove" -> {
            if (parent is ArrayNode) {
                val id = lastPart!!
                val index = parent.elements().indexOfFirst { it.get("id")?.asText() == id }
                if (index >= 0) parent.remove(index)
            } else if (parent is ObjectNode) {
                parent.remove(lastPart)
            }
        }
        "replace" -> {
            if (parent is ObjectNode) {
                parent.set<JsonNode>(lastPart!!, objectMapper.valueToTree(change.newValue))
            } else {
                throw IllegalArgumentException("Replace operation requires an object parent")
            }
        }
        "add" -> {
            if (parent is ArrayNode) {
                val newValue = (change.newValue as? Map<*, *>)?.toMutableMap()?.apply { put("id", lastPart) }
                    ?: throw IllegalArgumentException("Add to array requires a map value with id")
                parent.add(objectMapper.valueToTree(newValue))
            } else if (parent is ObjectNode) {
                parent.set<JsonNode>(lastPart!!, objectMapper.valueToTree(change.newValue))
            }
        }
        else -> throw UnsupportedOperationException("Operation ${change.op} not supported")
    }

    return root
}

@Test
fun `test delete first and update second with id in path including old and new values`() {
    val oldPerson = Person(
        id = "1",
        name = "Alice",
        toys = listOf(
            Toy("toy1", "Car"),
            Toy("toy2", "Doll")
        )
    )
    val newPerson = Person(
        id = "1",
        name = "Alice",
        toys = listOf(
            Toy("toy2", "Robot")
        )
    )
    val currentPerson = Person(
        id = "1",
        name = "Alice",
        toys = listOf(
            Toy("toy1", "Car"),
            Toy("toy2", "Doll")
        )
    )

    val oldNode = objectMapper.valueToTree(oldPerson)
    val newNode = objectMapper.valueToTree(newPerson)
    val currentNode = objectMapper.valueToTree(currentPerson)

    val changes = diffService.computeDiff(oldNode, newNode)
    assertEquals(2, changes.size)
    val removeChange = changes.find { it.op == "remove" }
    val replaceChange = changes.find { it.op == "replace" }
    assertEquals(Change("remove", "/toys/toy1", mapOf("id" to "toy1", "name" to "Car"), null), removeChange)
    assertEquals(Change("replace", "/toys/toy2/name", "Doll", "Robot"), replaceChange)

    val updatedNode = applyChangesCustom(changes, currentNode, objectMapper)
    val updatedPerson = objectMapper.treeToValue(updatedNode, Person::class.java)
    assertEquals(newPerson, updatedPerson)
}

data class Person(val id: String, val name: String, val toys: List<Toy>)
data class Toy(val id: String, val name: String)

fun applyChangesCustom(changes: List<Change>, currentNode: JsonNode, objectMapper: ObjectMapper): JsonNode {
    var node = currentNode
    for (change in changes) {
        node = applyChangeCustom(change, node, objectMapper)
    }
    return node
}
