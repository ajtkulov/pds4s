package pds.lsh

trait MinHashTrieStrategy {
  def accept(curLength: Int, totalLength: Int, curErrors: Int, totalErrors: Int): Boolean
}

object MinHashTrieStrategy extends MinHashTrieStrategy {
  override def accept(curLength: Int, totalLength: Int, curErrors: Int, totalErrors: Int): Boolean = {
    curLength * totalErrors >= (curErrors - 1) * totalLength && curErrors <= totalErrors
  }
}

object ExactMinHashTrieStrategy extends MinHashTrieStrategy {
  override def accept(curLength: Int, totalLength: Int, curErrors: Int, totalErrors: Int): Boolean = {
    curErrors == 0
  }
}
