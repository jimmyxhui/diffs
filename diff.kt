@Target(AnnotationTarget.FIELD, AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.RUNTIME)
annotation class SkipDiff

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document

@Document(collection = "persons")
data class Person(
    @Id val id: String,
    val version: Long = 0,
    val name: String,
    val age: Int,
    val pets: Set<Pet> = emptySet(),
    val attributes: Map<String, String> = emptyMap(),
    val toys: MutableList<Toy> = mutableListOf(),
    @SkipDiff val lastModified: Long = System.currentTimeMillis()
)

sealed class Pet {
    abstract val id: String
    abstract val name: String
    abstract val age: Int
}

data class Cat(
    override val id: String,
    override val name: String,
    override val age: Int,
    val lives: Int,
    @SkipDiff val secretNickname: String? = null
) : Pet()

data class Dog(
    override val id: String,
    override val name: String,
    override val age: Int,
    val toys: Set<Toy> = emptySet()
) : Pet()

data class Toy(
    val id: String,
    val name: String,
    val cost: Double
)

@Document(collection = "person_diffs")
data class PersonDiff(
    @Id val id: String,
    val personId: String,
    val version: Long,
    val changes: List<Change>
)

data class Change(
    val op: String,
    val path: String,
    val value: Any? = null,
    val itemId: String? = null
)

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ser.BeanPropertyFilter
import com.fasterxml.jackson.databind.ser.FilterProvider
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class JacksonConfig {

    @Bean
    fun objectMapper(): ObjectMapper {
        val mapper = ObjectMapper()
        val filter = SimpleBeanPropertyFilter.serializeAllExcept { prop ->
            prop.member?.hasAnnotation(SkipDiff::class.java) == true
        }
        val filterProvider = SimpleFilterProvider().addFilter("skipDiffFilter", filter)
        mapper.setFilterProvider(filterProvider)
        mapper.addMixIn(Any::class.java, SkipDiffMixin::class.java)
        return mapper
    }
}

@JsonFilter("skipDiffFilter")
interface SkipDiffMixin

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.fge.jsonpatch.JsonPatch
import com.github.fge.jsonpatch.diff.JsonDiff
import org.springframework.stereotype.Service

@Service
class DiffService(private val objectMapper: ObjectMapper) {

    fun computeDiff(oldObj: Any, newObj: Any): List<Change> = with(objectMapper) {
        val oldNode = valueToTree<JsonNode>(oldObj)
        val newNode = valueToTree<JsonNode>(newObj)
        JsonDiff.asJsonPatch(oldNode, newNode).map { operation ->
            val pathParts = operation.path.split("/").filter { it.isNotEmpty() }
            val itemId = findItemId(pathParts, newNode) ?: findItemId(pathParts, oldNode)
            Change(operation.op, operation.path, operation.value?.let { treeToValue(it, Any::class.java) }, itemId)
        }
    }

    fun toJsonPatch(changes: List<Change>, currentNode: JsonNode): JsonPatch {
        val reorderedChanges = changes.map { change ->
            val pathParts = change.path.split("/").filter { it.isNotEmpty() }
            val parentPath = pathParts.dropLast(1).joinToString("/")
            val lastPart = pathParts.last()
            if (parentPath.isNotEmpty() && currentNode.at(parentPath).isArray && change.itemId != null) {
                val targetIndex = currentNode.at(parentPath).elements()
                    .indexOfFirst { it.get("id")?.asText() == change.itemId }
                val newPath = if (targetIndex >= 0) "$parentPath/$targetIndex" else "$parentPath/-"
                change.copy(path = if (lastPart.matches("\\d+".toRegex())) newPath else "$newPath/$lastPart")
            } else change
        }
        val patchNode = reorderedChanges.map { change ->
            objectMapper.createObjectNode().apply {
                put("op", change.op)
                put("path", change.path)
                change.value?.let { set<JsonNode>("value", objectMapper.valueToTree(it)) }
            }
        }.let { objectMapper.createArrayNode().apply { addAll(it) } }
        return JsonPatch.fromJson(patchNode)
    }

    private fun findItemId(pathParts: List<String>, node: JsonNode): String? {
        var current = node
        for (i in 0 until pathParts.size - 1) {
            val part = pathParts[i]
            current = if (part.matches("\\d+".toRegex()) && current.isArray) current.get(part.toInt()) ?: return null
            else current.get(part) ?: return null
        }
        return current.takeIf { it.isObject && it.has("id") }?.get("id")?.asText()
    }
}

import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.util.UUID

@Service
class PersonService(
    private val personRepository: MongoRepository<Person, String>,
    private val diffRepository: MongoRepository<PersonDiff, String>,
    private val diffService: DiffService,
    private val kafkaTemplate: KafkaTemplate<String, PersonDiff>,
    private val objectMapper: ObjectMapper
) {

    fun updatePerson(personId: String, patch: Map<String, Any>): Person {
        val oldPerson = personRepository.findById(personId).orElseThrow { IllegalArgumentException("Person not found") }
        val oldNode = objectMapper.valueToTree<JsonNode>(oldPerson)
        val patchNode = objectMapper.valueToTree<JsonNode>(patch)
        val jsonPatch = JsonDiff.asJsonPatch(oldNode, patchNode)
        val newNode = jsonPatch.apply(oldNode)
        val newPerson = objectMapper.treeToValue(newNode, Person::class.java).copy(version = oldPerson.version + 1)

        val changes = diffService.computeDiff(oldPerson, newPerson)
        val diff = PersonDiff(UUID.randomUUID().toString(), personId, newPerson.version, changes)

        diffRepository.save(diff)
        kafkaTemplate.send("person-diffs", diff.personId, diff)
        return personRepository.save(newPerson)
    }

    fun reconstructVersion(personId: String, targetVersion: Long): Person {
        val basePerson = personRepository.findById(personId).orElseThrow { IllegalArgumentException("Person not found") }
        require(basePerson.version >= targetVersion) { "Target version exceeds current version" }

        val diffs = diffRepository.findByPersonIdAndVersionLessThanEqual(personId, targetVersion).sortedBy { it.version }
        return diffs.fold(objectMapper.valueToTree<JsonNode>(basePerson)) { current, diff ->
            diffService.toJsonPatch(diff.changes, current).apply(current)
        }.let { objectMapper.treeToValue(it, Person::class.java) }
    }

    fun compareVersions(personId: String, fromVersion: Long, toVersion: Long): List<Change> {
        val fromPerson = reconstructVersion(personId, fromVersion)
        val toPerson = reconstructVersion(personId, toVersion)
        return diffService.computeDiff(fromPerson, toPerson)
    }
}

import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/persons")
class PersonController(private val personService: PersonService) {

    @GetMapping("/{id}/compare")
    fun compareVersions(
        @PathVariable id: String,
        @RequestParam fromVersion: Long,
        @RequestParam toVersion: Long
    ): List<Change> = personService.compareVersions(id, fromVersion, toVersion)
}

import com.fasterxml.jackson.databind.JsonNode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.mongodb.repository.MongoRepository

@SpringBootTest
class DiffServiceTest(
    private val diffService: DiffService,
    private val personService: PersonService,
    private val personRepository: MongoRepository<Person, String>,
    private val diffRepository: MongoRepository<PersonDiff, String>,
    private val objectMapper: ObjectMapper
) {

    @BeforeEach
    fun setUp() {
        personRepository.deleteAll()
        diffRepository.deleteAll()
    }

    @Test
    fun `computeDiff skips fields with SkipDiff annotation`() {
        val oldPerson = Person("1", 0, "Alice", 30, setOf(Cat("123", "Whiskers", 5, 9, "Kitty")))
        val newPerson = Person("1", 0, "Alice", 30, setOf(Cat("123", "Fluffy", 5, 9, "Meow")))

        val changes = diffService.computeDiff(oldPerson, newPerson)
        assertEquals(1, changes.size)
        assertEquals(Change("replace", "/pets/0/name", "Fluffy", "123"), changes[0]) // secretNickname skipped
    }

    @Test
    fun `updatePerson applies patch and ignores SkipDiff fields in diff`() {
        val initialPerson = Person("1", 0, "Alice", 30, emptySet(), emptyMap(), mutableListOf(), 1234567890L)
        personRepository.save(initialPerson)

        val patch = mapOf(
            "name" to "Bob",
            "lastModified" to 9876543210L,
            "pets" to listOf(mapOf("id" to "123", "type" to "Cat", "name" to "Whiskers", "age" to 5, "lives" to 9))
        )
        val updatedPerson = personService.updatePerson("1", patch)

        assertEquals(1, updatedPerson.version)
        assertEquals("Bob", updatedPerson.name)
        assertEquals(9876543210L, updatedPerson.lastModified)
        assertEquals(setOf(Cat("123", "Whiskers", 5, 9)), updatedPerson.pets)
        val diffs = diffRepository.findByPersonIdAndVersionLessThanEqual("1", 1)
        assertEquals(1, diffs.size)
        assertEquals(
            listOf(
                Change("replace", "/name", "Bob"),
                Change("add", "/pets/0", mapOf("id" to "123", "type" to "Cat", "name" to "Whiskers", "age" to 5, "lives" to 9), "123")
            ),
            diffs[0].changes
        ) // lastModified not in diff
    }

    @Test
    fun `reconstructVersion handles reordered list with id matching`() {
        val initialPerson = Person("1", 0, "Alice", 30, setOf(Cat("123", "Whiskers", 5, 9)), emptyMap(), mutableListOf(), 1234567890L)
        personRepository.save(initialPerson)

        val patch = mapOf(
            "pets" to listOf(
                mapOf("id" to "456", "type" to "Dog", "name" to "Rex", "age" to 3, "toys" to emptySet<Any>()),
                mapOf("id" to "123", "type" to "Cat", "name" to "Fluffy", "age" to 5, "lives" to 9)
            ),
            "lastModified" to 9876543210L
        )
        personService.updatePerson("1", patch)

        val v0 = personService.reconstructVersion("1", 0)
        val v1 = personService.reconstructVersion("1", 1)

        assertEquals(Person("1", 0, "Alice", 30, setOf(Cat("123", "Whiskers", 5, 9)), emptyMap(), mutableListOf(), 1234567890L), v0)
        assertEquals(Person("1", 1, "Alice", 30, setOf(Cat("123", "Fluffy", 5, 9), Dog("456", "Rex", 3)), emptyMap(), mutableListOf(), 9876543210L), v1)
    }

    @Test
    fun `compareVersions shows differences with id matching and SkipDiff`() {
        val initialPerson = Person("1", 0, "Alice", 30, emptySet(), emptyMap(), mutableListOf(), 1234567890L)
        personRepository.save(initialPerson)

        personService.updatePerson("1", mapOf("name" to "Bob", "lastModified" to 9876543210L))
        personService.updatePerson("1", mapOf("pets" to listOf(mapOf("id" to "123", "type" to "Cat", "name" to "Whiskers", "age" to 5, "lives" to 9))))

        val changes = personService.compareVersions("1", 0, 2)
        assertEquals(2, changes.size)
        assertTrue(changes.contains(Change("replace", "/name", "Bob")))
        assertTrue(changes.contains(Change("add", "/pets/0", mapOf("id" to "123", "type" to "Cat", "name" to "Whiskers", "age" to 5, "lives" to 9), "123")))
    }

    @Test
    fun `hybrid approach adjusts indices for reordered lists`() {
        val initialPerson = Person("1", 0, "Alice", 30, setOf(Cat("123", "Whiskers", 5, 9), Dog("456", "Rex", 3)), emptyMap(), mutableListOf())
        personRepository.save(initialPerson)

        // Reorder and modify
        val patch = mapOf(
            "pets" to listOf(
                mapOf("id" to "456", "type" to "Dog", "name" to "Rex", "age" to 3, "toys" to emptySet<Any>()),
                mapOf("id" to "123", "type" to "Cat", "name" to "Fluffy", "age" to 5, "lives" to 9)
            )
        )
        personService.updatePerson("1", patch)

        val v0 = personService.reconstructVersion("1", 0)
        val v1 = personService.reconstructVersion("1", 1)

        assertEquals(Person("1", 0, "Alice", 30, setOf(Cat("123", "Whiskers", 5, 9), Dog("456", "Rex", 3)), emptyMap(), mutableListOf()), v0)
        assertEquals(Person("1", 1, "Alice", 30, setOf(Cat("123", "Fluffy", 5, 9), Dog("456", "Rex", 3)), emptyMap(), mutableListOf()), v1)

        val changes = diffRepository.findByPersonIdAndVersionLessThanEqual("1", 1)[0].changes
        assertTrue(changes.contains(Change("replace", "/pets/0/name", "Fluffy", "123")))
    }
}
