package edu.berkeley.cs.amplab.adam.projections

import org.apache.avro.Schema
import scala.collection.JavaConversions._
import org.apache.avro.Schema.Field

/**
 * Avro utility object to create a projection of a Schema
 */
object Projection {

  private def createProjection(fullSchema: Schema, includedFields: Set[String]): Schema = {
    val projectedSchema = Schema.createRecord(fullSchema.getName, fullSchema.getDoc, fullSchema.getNamespace, fullSchema.isError)
    projectedSchema.setFields(fullSchema.getFields.filter(p => includedFields.contains(p.name)).map(p => new Field(p.name, p.schema, p.doc, p.defaultValue, p.order)))
    projectedSchema
  }

  def apply(includedFields: FieldValue*): Schema = {
    assert(!includedFields.isEmpty, "Can't project down to zero fields!")
    val baseSchema = includedFields.head.schema
    createProjection(baseSchema, includedFields.map(_.toString).toSet)
  }

}
