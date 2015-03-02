/**
* File that contains a reference assembly that can be broadcasted
*/
trait ReferenceFile {
	/**
   * Extract reference sequence from the .2bit data.
   *
   * @param region The desired ReferenceRegion to extract.
   * @return The reference sequence at the desired locus.
   */
  def extract(region: ReferenceRegion): String
}