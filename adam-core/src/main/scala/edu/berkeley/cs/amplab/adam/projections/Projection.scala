package edu.berkeley.cs.amplab.adam.projections

import org.apache.avro.Schema
import scala.collection.JavaConversions._
import org.apache.avro.Schema.Field

/**
 * Avro utility object to create a projection of a Schema
 */
object Projection {

  private def createProjection(fullSchema: Schema, fields: Set[String], exclude : Boolean = false): Schema = {
    val projectedSchema = Schema.createRecord(fullSchema.getName, fullSchema.getDoc, fullSchema.getNamespace, fullSchema.isError)
    projectedSchema.setFields(fullSchema.getFields.filter(createFilterPredicate(fields, exclude))
                    .map(p => new Field(p.name, p.schema, p.doc, p.defaultValue, p.order)))
    projectedSchema
  }

  private def createFilterPredicate(fieldNames : Set[String], exclude : Boolean = false): Field => Boolean = {
    val filterPred = (f : Field) => fieldNames.contains(f.name)
    val includeOrExlcude = ( contains : Boolean ) => if (exclude) !contains else contains
    filterPred.andThen(includeOrExlcude)
  }

  // TODO: Unify these various methods
  def apply(includedFields: FieldValue*): Schema = {
    assert(!includedFields.isEmpty, "Can't project down to zero fields!")
    Projection(false, includedFields:_*)
  }

  def apply(includedFields: Traversable[FieldValue]) : Schema = {
    assert(includedFields.size > 0, "Can't project down to zero fields!")
    val baseSchema = includedFields.head.schema
    createProjection(baseSchema, includedFields.map(_.toString).toSet, false)
  }

  def apply(exclude:Boolean, includedFields: FieldValue*): Schema = {
    val baseSchema = includedFields.head.schema
    createProjection(baseSchema, includedFields.map(_.toString).toSet, exclude)
  }
}

object Filter {

  def apply(excludeFields: FieldValue*): Schema = {
    Projection(true, excludeFields:_*)
  }
}
