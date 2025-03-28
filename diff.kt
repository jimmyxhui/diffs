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
            if (current.elements().all { it.isObject }) {
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
                val index = part.toIntOrNull() ?: throw IllegalArgumentException("Invalid index $part at ${pathParts.take(i + 1).joinToString("/", "/")}")
                if (index < current.size()) {
                    current = current.get(index)
                } else if (isLastPart && change.op == "add") {
                    break
                } else {
                    throw IllegalArgumentException("Index $index out of bounds at ${pathParts.take(i + 1).joinToString("/", "/")}")
                }
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
                if (parent.elements().all { it.isObject }) {
                    val id = lastPart!!
                    val index = parent.elements().indexOfFirst { it.get("id")?.asText() == id }
                    if (index >= 0) parent.remove(index)
                } else {
                    val index = lastPart!!.toInt()
                    if (index >= 0 && index < parent.size()) parent.remove(index)
                }
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
                if (parent.elements().all { it.isObject }) {
                    val newValue = (change.newValue as? Map<*, *>)?.toMutableMap()?.apply { put("id", lastPart) }
                        ?: throw IllegalArgumentException("Add to object array requires a map value with id")
                    parent.add(objectMapper.valueToTree(newValue))
                } else {
                    parent.add(objectMapper.valueToTree(change.newValue))
                }
            } else if (parent is ObjectNode) {
                parent.set<JsonNode>(lastPart!!, objectMapper.valueToTree(change.newValue))
            }
        }
        else -> throw UnsupportedOperationException("Operation ${change.op} not supported")
    }

    return root
}
