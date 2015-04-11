package krakken.macros

import com.novus.salat.Grater
import krakken.model.{InjectedTypeHint, TypeHint}

import scala.reflect.macros.whitebox
import scala.util.parsing.json.{JSONArray, JSONObject}

private [macros] object Helpers {

  import scala.util.parsing.json.JSON

  def prettify(rawJson: String): String = _prettify(JSON.parseRaw(rawJson), 0)

  private def _prettify(parsed: Option[Any], l: Int): String = {
    val indent = (for (i <- List.range(0, l)) yield "  ").mkString
    parsed match {
      case Some(o: JSONObject) => List("{",
        o.obj.keys.map(key => indent + " " + "" + key + ":" + _prettify(o.obj.get(key), l + 1)).mkString(",\n"),
        indent + "}").mkString("\n")
      case Some(a: JSONArray) if a.list.isEmpty => "[]"
      case Some(a: JSONArray) => List("[",
        a.list.map(v => indent + " " + _prettify(Some(v), l + 1)).mkString(",\n"),
        indent + "]").mkString("\n")
      case Some(s: String) => s
      case None => "null"
      case any => any.toString
    }
  }

}

private [macros] object Implementations {

  def printExpr_impl(c: whitebox.Context)(param: c.Expr[Any]): c.Expr[Unit] = {
    import c.universe._
    val paramRepTree = Literal(Constant(show(param.tree)))
    val paramRepExpr = c.Expr[String](paramRepTree)
    reify {
      println(paramRepExpr.splice + " = " + param.splice)
    }
  }

  def debug_impl(c: whitebox.Context)(params: c.Expr[Any]*): c.Expr[Unit] = {
    import c.universe._

    val trees = params.map { param =>
      param.tree match {
        case Literal(Constant(const)) => reify(print(param.splice)).tree
        case _ => {
          val paramRep = show(param.tree)
          val paramRepTree = Literal(Constant(paramRep))
          val paramRepExpr = c.Expr[String](paramRepTree)
          reify {
            print(paramRepExpr.splice + " = " + param.splice)
          }.tree
        }
      }
    }

    val treesWithSeparators = trees.foldLeft(Seq[c.universe.Tree]()) {
      (acc, p) => acc :+ p :+ reify(print(", ")).tree
    }.dropRight(1).:+(reify(println()).tree)

    c.Expr[Unit](Block(treesWithSeparators.toList, Literal(Constant())))
  }

  //TODO for now limited to case classes inspector
  //TODO refactor this to generate arrayNode better
  //divide fields in param constructors public, private. passed as implicit and normal other fields created inside
  //divide methods as public and private
  //give superClass and className
  def inspect_impl[T: c.WeakTypeTag](c: whitebox.Context)(obj: c.Expr[T]): c.Expr[Unit] = {
    import c.universe._

    val sym = c.weakTypeOf[T].typeSymbol
    if (!sym.isClass /*|| !sym.asClass.isCaseClass*/ ) c.abort(c.enclosingPosition, s"$sym is not a class instance")

    val declarations = sym.typeSignature.decls.sorted.toList

    def generateJsonArray(l: List[c.universe.Symbol], nodeName: String) = {
      l.foldLeft(" \"" + nodeName + "\" : [  ") { (acc, f) =>
        acc + "\"" + f.name.decodedName.toString + "\", "
      }.dropRight(2) + "]"
    }

    val representation = "{" +
      generateJsonArray(declarations.filter(_.asTerm.isParamAccessor), "constructors") + "," +
      "\"accessors\":{" +
      generateJsonArray(declarations.filter { x => x.isTerm && x.asTerm.isGetter}, "getters") + "," +
      generateJsonArray(declarations.filter { x => x.isTerm && x.asTerm.isSetter}, "setters") + "}" + "," +
      generateJsonArray(declarations.filter { x => x.isMethod}, "methods") +
      "}"

    reify {
      println(c.Expr[String](Literal(Constant(Helpers.prettify(representation)))).splice)
    }

    //    println("DECLARATIONS: " + declarations.toString)

    //    val caseAccessorPrintExprTree =
    //      caseAccessors.foldLeft(Seq[c.Tree](reify{ print( c.Expr[String](Literal(Constant(" Class constructors : { "))).splice )}.tree)){
    //        (acc,d) =>
    //          acc.:+( reify { print( c.Expr[String](Literal(Constant(d.name.decodedName.toString))).splice)}.tree)
    //            .:+( reify { print( c.Expr[String](Literal(Constant(","))).splice)}.tree)
    //      }.dropRight(1)
    //        .:+( reify { println( c.Expr[String](Literal(Constant(" } "))).splice)}.tree )
    //    println("TREES: " + trees)

    //
    //
    //    val trees:Seq[Tree] = declarations.head._2.map{ field =>
    //      reify { println( c.Expr[String](Literal(Constant(field.name.decodedName.toString))).splice) }.tree
    //    }

    //    c.Expr[Unit](Block(caseAccessorPrintExprTree.toList,Literal(Constant())))

  }


  def grateSealed_impl[A : c.WeakTypeTag](c: whitebox.Context)(ctx: c.Expr[com.novus.salat.Context])
  : c.Expr[Map[TypeHint, Grater[_ <: A]]] = {
    import c.universe._

    implicit val liftableTypeHint: Liftable[InjectedTypeHint] = Liftable[InjectedTypeHint] { hint ⇒
        q"_root_.krakken.model.InjectedTypeHint(${hint.hint})"
      }

    val symbol = c.weakTypeOf[A].typeSymbol

    val internal = symbol.asInstanceOf[scala.reflect.internal.Symbols#Symbol]

    val descendants = internal.sealedDescendants.map(_.asInstanceOf[Symbol]) - symbol

    val descendantsExpr = c.Expr[List[TypeHint]] {
      q"""{List(${descendants.map( d ⇒ InjectedTypeHint(d.fullName)).toList}).flatten}"""
    }

    reify{
      val $descendants = descendantsExpr.splice
      val $ctx = ctx.splice
      var $n = $descendants.length
      val $graters = scala.collection.mutable.Map.empty[TypeHint, Grater[_ <: A]]
      while ($n != 0) {
        $n -= 1
        val $desc = $descendants($n)
        $graters.update($desc, $ctx.lookup($desc.hint).asInstanceOf[Grater[_ <: A]])
      }
      $graters.toMap
    }
  }

}

object Macros {

  import scala.language.experimental.macros

  /**
   * Prints the expression an its result.
   * Example:
   * val x = 10
   * printExpr(x*10) //outputs Main.this.x.*(10) = 100
   *
   * Explanation:
   * show(param.tree) stringifies the tree of the passed expression.
   * We need to pass a splicee Expr[T] to println in order to execute it
   * inside reify. We do this by manually creating a tree and pass it
   * to c.Expr[String].
   */
  def printExpr(param: Any): Unit = macro Implementations.printExpr_impl

  /**
   * Takes a Sealed Trait and gives you back a grater for
   * each of the descendants.
   *
   * @return Map[TypeHint, com.novus.salat.Grater[_]
   */
  def grateSealed[A](implicit ctx: com.novus.salat.Context)
  : Map[TypeHint, Grater[_ <: A]] = macro Implementations.grateSealed_impl[A]

  /**
   * TODO finish
   */
  def inspectMembers[T](obj: T): Unit = macro Implementations.inspect_impl[T]

}
