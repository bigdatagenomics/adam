package edu.berkeley.cs.amplab.adam.projections

import org.apache.avro.Schema
import scala.collection.JavaConversions._
import org.apache.avro.Schema.Field
import edu.berkeley.cs.amplab.adam.predicates.ADAMRecordField
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord

/**
 * Avro utility object to create a projection of a Schema
 */
object Projection {

  private def createProjection(fullSchema: Schema, includedFields: Set[String]): Schema = {
    val projectedSchema = Schema.createRecord(fullSchema.getName, fullSchema.getDoc, fullSchema.getNamespace, fullSchema.isError)
    projectedSchema.setFields(fullSchema.getFields.filter(p => includedFields.contains(p.name)).map(p => new Field(p.name, p.schema, p.doc, p.defaultValue, p.order)))
    projectedSchema
  }

  def apply(includedFields: ADAMRecordField.Value*): Schema = {
    createProjection(ADAMRecord.SCHEMA$, includedFields.map(_.toString).toSet)
  }

}
