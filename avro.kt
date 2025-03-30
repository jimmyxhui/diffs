package com.example.demo

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import graphql.schema.idl.RuntimeWiring
import graphql.schema.idl.SchemaGenerator
import graphql.schema.idl.SchemaParser
import graphql.schema.idl.TypeDefinitionRegistry
import org.apache.avro.Schema
import org.springframework.core.io.ClassPathResource
import java.io.File

data class AvroField(
    val name: String,
    val type: Any,
    val default: Any? = null
)

data class AvroRecord(
    val type: String = "record",
    val name: String,
    val fields: List<AvroField>
)

object AvroSchemaGenerator {

    private val objectMapper = ObjectMapper().registerKotlinModule()

    // Maps GraphQL scalar types to Avro types
    private val typeMapping = mapOf(
        "ID" to "string",
        "String" to "string",
        "Int" to "int",
        "Float" to "float",
        "Boolean" to "boolean"
    )

    fun generateAvroFromGraphql(graphqlFile: String): List<AvroRecord> {
        // Parse GraphQL schema
        val schemaFile = ClassPathResource(graphqlFile).inputStream.bufferedReader().use { it.readText() }
        val typeRegistry = SchemaParser().parse(schemaFile)
        val graphqlSchema = SchemaGenerator().makeExecutableSchema(typeRegistry, RuntimeWiring.newRuntimeWiring().build())

        val avroRecords = mutableListOf<AvroRecord>()

        // Process each type definition
        typeRegistry.types().values.forEach { type ->
            when {
                type.isObjectTypeDefinition || type.isInterfaceTypeDefinition -> {
                    val fields = type.children.filterIsInstance<graphql.language.FieldDefinition>().map { field ->
                        val avroType = convertGraphqlTypeToAvro(field.type, typeRegistry)
                        AvroField(
                            name = field.name,
                            type = if (field.type.isNonNull()) avroType else listOf("null", avroType),
                            default = if (field.type.isNonNull()) null else null
                        )
                    }
                    avroRecords.add(
                        AvroRecord(
                            name = type.name,
                            fields = fields
                        )
                    )
                }
            }
        }

        // Handle polymorphic fields (e.g., Query.hero)
        typeRegistry.types().values.filter { it.isObjectTypeDefinition }.forEach { type ->
            type.children.filterIsInstance<graphql.language.FieldDefinition>().forEach { field ->
                val fieldType = graphqlSchema.getType(field.type.name())
                if (fieldType?.isInterface == true) {
                    val implementingTypes = typeRegistry.types().values
                        .filter { it.isObjectTypeDefinition && it.implementsInterface(field.type.name()) }
                        .map { it.name }
                    val avroFieldType = if (field.type.isNonNull()) implementingTypes else listOf("null") + implementingTypes
                    avroRecords.find { it.name == type.name }?.fields?.find { it.name == field.name }?.let {
                        it.type = avroFieldType
                    }
                }
            }
        }

        return avroRecords
    }

    private fun convertGraphqlTypeToAvro(type: graphql.language.Type<*>, typeRegistry: TypeDefinitionRegistry): Any {
        val typeName = type.name()
        return when {
            type.isNonNull() -> convertGraphqlTypeToAvro(type.children.first() as graphql.language.Type<*>, typeRegistry)
            typeMapping.containsKey(typeName) -> typeMapping[typeName]!!
            typeRegistry.getType(typeName).isPresent -> typeName // Reference to another record
            else -> throw IllegalArgumentException("Unsupported GraphQL type: $typeName")
        }
    }

    private fun graphql.language.Type<*>.name(): String = when (this) {
        is graphql.language.TypeName -> this.name
        is graphql.language.NonNullType -> (this.type as graphql.language.TypeName).name
        is graphql.language.ListType -> throw UnsupportedOperationException("Lists not yet supported")
        else -> throw IllegalArgumentException("Unknown type: $this")
    }

    private fun graphql.language.Type<*>.isNonNull(): Boolean = this is graphql.language.NonNullType

    fun implementsInterface(typeDef: graphql.language.TypeDefinition<*>, interfaceName: String): Boolean {
        return (typeDef as? graphql.language.ObjectTypeDefinition)?.implements?.any {
            (it as? graphql.language.TypeName)?.name == interfaceName
        } == true
    }

    fun toJson(avroRecords: List<AvroRecord>): String = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(avroRecords)

    fun toAvroSchema(avroRecords: List<AvroRecord>): List<Schema> = avroRecords.map { Schema.Parser().parse(objectMapper.writeValueAsString(it)) }
}

@SpringBootApplication
class DemoApplication {
    @PostConstruct
    fun generateSchemas() {
        val avroRecords = AvroSchemaGenerator.generateAvroFromGraphql("schema.graphqls")
        val json = AvroSchemaGenerator.toJson(avroRecords)
        println("Generated Avro Schemas:\n$json")

        // Optionally, write to file
        File("src/main/resources/avro-schemas.json").writeText(json)

        // Convert to Avro Schema objects if needed
        val schemas = AvroSchemaGenerator.toAvroSchema(avroRecords)
    }
}

fun main(args: Array<String>) {
    runApplication<DemoApplication>(*args)
}
