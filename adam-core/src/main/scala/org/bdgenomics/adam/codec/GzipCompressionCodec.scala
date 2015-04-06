package org.bdgenomics.adam.codec
/**
 * Created by alexsalazar on 3/6/15.
 * Note: trait and class were copied from maropu's pull request:
 *  https://github.com/maropu/spark/commit/2b8c62503c7b41d814d67c7034084c52856388e4
 */
import java.io.{ InputStream, OutputStream }
import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import java.util.zip.{ GZIPInputStream, GZIPOutputStream }
import org.apache.spark.io.CompressionCodec

@DeveloperApi
class GzipCompressionCodec(conf: SparkConf) extends CompressionCodec {
  override def compressedOutputStream(s: OutputStream): OutputStream = {
    val blockSize = conf.getInt("spark.io.compression.gzip.block.size", 32768)
    new GZIPOutputStream(s, blockSize)
  }
  override def compressedInputStream(s: InputStream): InputStream = new GZIPInputStream(s)
}
