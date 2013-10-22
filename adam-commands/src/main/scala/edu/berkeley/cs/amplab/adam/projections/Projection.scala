package edu.berkeley.cs.amplab.adam.projections

import org.apache.avro.Schema
import scala.collection.JavaConversions._

/**
 * Avro utility object to create a projection of a Schema, e.g.
 *
 * val schema = Projection(ADAMRecord.SCHEMA$, Set("referenceName", "readName"))
 */
object Projection {

  def apply(fullSchema: Schema, includedFields: Set[String]): Schema = {
    val projectedSchema = Schema.createRecord(fullSchema.getName, fullSchema.getDoc, fullSchema.getNamespace, fullSchema.isError)
    projectedSchema.setFields(fullSchema.getFields.filter(includedFields.contains(_)))
    projectedSchema
  }

}
