import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.flipkart.zjsonpatch.JsonDiff

class DiffGenerator(private val objectMapper: ObjectMapper = jacksonObjectMapper()) {

    fun <T : Any> computeDiff(oldObj: T, newObj: T): List<Change> {
        val oldJson = objectMapper.valueToTree<JsonNode>(oldObj)
        val newJson = objectMapper.valueToTree<JsonNode>(newObj)
        val patch = JsonDiff.asJson(oldJson, newJson)
        return transformPatchToChanges(patch, oldJson, newJson)
    }

    private fun transformPatchToChanges(patch: JsonNode, oldJson: JsonNode, newJson: JsonNode): List<Change> {
        val changes = mutableListOf<Change>()
        patch.forEach { operation ->
            val op = operation["op"].asText()
            val path = operation["path"].asText()
            val value = operation["value"]?.let { objectMapper.treeToValue(it, Any::class.java) }

            // Extract itemIds by traversing the path in the original JSON
            val itemIds = extractItemIds(path, oldJson, newJson, op)
            changes.add(Change(op = op, path = path, value = value, itemIds = itemIds))
        }
        return changes
    }

    private fun extractItemIds(path: String, oldJson: JsonNode, newJson: JsonNode, op: String): List<String> {
        val itemIds = mutableListOf<String>()
        val segments = path.split("/").filter { it.isNotEmpty() }
        var currentNode = if (op == "add") newJson else oldJson // Use newJson for "add" ops, oldJson otherwise

        for (i in segments.indices) {
            val segment = segments[i]
            if (segment.matches(Regex("\\d+"))) { // Array index
                val index = segment.toInt()
                if (currentNode.isArray && index < currentNode.size()) {
                    val element = currentNode[index]
                    if (element.isObject && element.has("id")) {
                        itemIds.add(element["id"].asText())
                    }
                    currentNode = element // Move deeper into the structure
                }
            } else if (currentNode.isObject && currentNode.has(segment)) {
                currentNode = currentNode[segment] // Move to the named field
            }
        }
        return itemIds
    }
}

import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import kotlin.reflect.KClass

@Service
class DocumentService(
    private val diffRepository: MongoRepository<VersionDiff, String>,
    private val kafkaTemplate: KafkaTemplate<String, List<Change>>,
    private val diffGenerator: DiffGenerator
) {

    fun <T : Any> updateDocument(
        repository: MongoRepository<T, String>,
        collectionName: String,
        documentId: String,
        updatedDoc: T,
        versionField: String = "version"
    ): T {
        val oldDoc = repository.findById(documentId)
            .orElseThrow { IllegalArgumentException("Document not found in $collectionName") }

        val savedDoc = repository.save(updatedDoc)
        val changes = diffGenerator.computeDiff(oldDoc, savedDoc)

        val oldVersion = oldDoc.getVersion(versionField)
        val newVersion = savedDoc.getVersion(versionField)

        val versionDiff = VersionDiff(
            documentId = documentId,
            collectionName = collectionName,
            fromVersion = oldVersion,
            toVersion = newVersion,
            changes = changes
        )
        diffRepository.save(versionDiff)

        kafkaTemplate.send("$collectionName-updates", documentId, changes)

        return savedDoc
    }

    fun <T : Any> compareVersions(
        documentId: String,
        collectionName: String,
        fromVersion: Long,
        toVersion: Long,
        clazz: KClass<T>
    ): List<Change> {
        if (fromVersion >= toVersion) throw IllegalArgumentException("fromVersion must be less than toVersion")

        val diffs = diffRepository.findAllByDocumentIdAndCollectionNameAndFromVersionGreaterThanEqualOrderByFromVersion(
            documentId, collectionName, fromVersion
        ).filter { it.toVersion <= toVersion }

        if (diffs.isEmpty()) return emptyList()

        // Merge changes (simple concatenation for now)
        return diffs.flatMap { it.changes }
    }

    private fun Any.getVersion(versionField: String): Long {
        val json = objectMapper.valueToTree<JsonNode>(this)
        return json[versionField].asLong()
    }
}

interface VersionDiffRepository : MongoRepository<VersionDiff, String> {
    fun findAllByDocumentIdAndCollectionNameAndFromVersionGreaterThanEqualOrderByFromVersion(
        documentId: String,
        collectionName: String,
        fromVersion: Long
    ): List<VersionDiff>
}
