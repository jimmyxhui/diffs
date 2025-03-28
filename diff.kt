import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.flipkart.zjsonpatch.JsonPatch

fun applyChange(change: Change, currentNode: JsonNode, objectMapper: ObjectMapper): JsonNode {
    val pathParts = change.path.split("/").filter { it.isNotEmpty() }
    var adjustedPath = ""
    var current = currentNode
    var itemIdIndex = 0

    // Adjust the path for all operations
    for (i in pathParts.indices) {
        val part = pathParts[i]
        if (current.isArray && (part.matches("\\d+".toRegex()) || part == "-")) {
            val id = change.itemIds.getOrNull(itemIdIndex++)
            if (id != null && part != "-") { // "-" means append, no id needed
                val targetIndex = current.elements().asSequence()
                    .mapIndexed { index, jsonNode -> Pair(index, jsonNode) }
                    .firstOrNull { it.second.get("id")?.asText() == id }?.first
                    ?: throw IllegalArgumentException("Object with id $id not found at /$adjustedPath")
                adjustedPath += "/$targetIndex"
                current = current.get(targetIndex)
                    ?: throw IllegalArgumentException("Invalid index at /$adjustedPath")
            } else {
                adjustedPath += "/$part" // Use original index or "-" for append
                if (part != "-") current = current.get(part.toInt())
                    ?: throw IllegalArgumentException("Invalid index at /$adjustedPath")
            }
        } else {
            adjustedPath += "/$part"
            current = current.get(part) ?: if (i == pathParts.size - 1 && change.op == "add") {
                break // Allow adding a new field
            } else {
                throw IllegalArgumentException("Field $part not found at /$adjustedPath")
            }
        }
    }

    // Prepare the patch operation
    val patchOperation = objectMapper.createObjectNode().apply {
        put("op", change.op)
        put("path", adjustedPath)
        if (change.op == "add" && change.value is Map<*, *>) {
            val id = change.itemIds.getOrNull(0)
            val valueMap = change.value as Map<String, Any>
            val newValue = if (id != null) valueMap.toMutableMap().apply { put("id", id) } else valueMap
            set<JsonNode>("value", objectMapper.valueToTree(newValue))
        } else {
            change.value?.let { set<JsonNode>("value", objectMapper.valueToTree(it)) }
        }
    }
    val patchArray = objectMapper.createArrayNode().apply { add(patchOperation) }

    // Apply the patch
    return JsonPatch.apply(patchArray, currentNode)
}
