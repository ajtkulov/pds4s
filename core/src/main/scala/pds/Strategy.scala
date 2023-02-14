package pds

trait Strategy {
  def acceptInsert(context: ds.Error): Boolean

  def acceptReplace(context: ds.Error): Boolean

  def acceptDelete(context: ds.Error): Boolean

  def acceptSwap(context: ds.Error): Boolean
}

trait OneRuleStrategy extends Strategy {
  def accept(context: ds.Error): Boolean

  def acceptInsert(context: ds.Error): Boolean = accept(context)

  def acceptReplace(context: ds.Error): Boolean = accept(context)

  def acceptDelete(context: ds.Error): Boolean = accept(context)

  def acceptSwap(context: ds.Error): Boolean = accept(context)
}

object TrivialStrategy extends OneRuleStrategy {
  override def accept(context: ds.Error): Boolean = true
}

case class ProhibitChangesInShortWordsStrategy(input: String) extends OneRuleStrategy {
  lazy val split = input.split(" ")
  val shortWords: Set[Int] = split.zipWithIndex.filter { x => x._1.length <= 3 }.map(x => x._2).toSet

  override def accept(context: ds.Error): Boolean = !shortWords.contains(context.contextWordIdx)
}

case class ProhibitBigChangesInShortWordsStrategy(input: String) extends OneRuleStrategy {
  lazy val split = input.split(" ")
  val shortWords: Set[Int] = split.zipWithIndex.filter { x => x._1.length <= 3 }.map(x => x._2).toSet

  override def accept(context: ds.Error): Boolean = !shortWords.contains(context.contextWordIdx) ||
    (shortWords.contains(context.contextWordIdx) && context.insertByWord(context.contextWordIdx) + context.replaceByWord(context.contextWordIdx) == 0)
}

case class ProhibitPrefixChangesStrategy(prefixLength: Int) extends OneRuleStrategy {
  override def accept(context: ds.Error): Boolean = context.positionInWord >= prefixLength
}

case class AndStrategy(fst: Strategy, snd: Strategy) extends Strategy {
  override def acceptInsert(context: ds.Error): Boolean = fst.acceptInsert(context) && snd.acceptInsert(context)

  override def acceptReplace(context: ds.Error): Boolean = fst.acceptReplace(context) && snd.acceptReplace(context)

  override def acceptDelete(context: ds.Error): Boolean = fst.acceptDelete(context) && snd.acceptDelete(context)

  override def acceptSwap(context: ds.Error): Boolean = fst.acceptSwap(context) && snd.acceptSwap(context)
}

case class OrStrategy(fst: Strategy, snd: Strategy) extends Strategy {
  override def acceptInsert(context: ds.Error): Boolean = fst.acceptInsert(context) || snd.acceptInsert(context)

  override def acceptReplace(context: ds.Error): Boolean = fst.acceptReplace(context) || snd.acceptReplace(context)

  override def acceptDelete(context: ds.Error): Boolean = fst.acceptDelete(context) || snd.acceptDelete(context)

  override def acceptSwap(context: ds.Error): Boolean = fst.acceptSwap(context) || snd.acceptSwap(context)
}

case class NotStrategy(strategy: Strategy) extends Strategy {
  override def acceptInsert(context: ds.Error): Boolean = !strategy.acceptInsert(context)

  override def acceptReplace(context: ds.Error): Boolean = !strategy.acceptReplace(context)

  override def acceptDelete(context: ds.Error): Boolean = !strategy.acceptDelete(context)

  override def acceptSwap(context: ds.Error): Boolean = !strategy.acceptSwap(context)
}

case class AllowSwapAndDeleteStrategy(strategy: Strategy) extends Strategy {
  override def acceptInsert(context: ds.Error): Boolean = strategy.acceptInsert(context)

  override def acceptReplace(context: ds.Error): Boolean = strategy.acceptReplace(context)

  override def acceptDelete(context: ds.Error): Boolean = context.positionInWord > 0

  override def acceptSwap(context: ds.Error): Boolean = context.positionInWord > 0
}
