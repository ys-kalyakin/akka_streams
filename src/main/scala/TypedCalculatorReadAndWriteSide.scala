import akka.{NotUsed, actor}
import akka.actor.typed.{ActorSystem, Behavior, Props}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.stream.{ClosedShape, FlowShape, Graph, SinkShape, SourceShape, UniformFanOutShape}
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.stream.alpakka.slick.scaladsl.{Slick, SlickSession}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source}
import akka_typed.TypedCalculatorWriteSide.{Add, Added, Command, Divide, Divided, Multiplied, Multiply}
import scalikejdbc.DB.using
import scalikejdbc.{ConnectionPool, DB}
import akka_typed.CalculatorRepository.{Result, db, getLatestsOffsetAndResultSlick, profile, updatedResultAndOffset}
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.concurrent.duration.DurationInt

object  akka_typed{

  trait CborSerialization

  val persId = PersistenceId.ofUniqueId("001")

  object TypedCalculatorWriteSide{
    sealed trait Command
    case class Add(amount: Double) extends Command
    case class Multiply(amount: Double) extends Command
    case class Divide(amount: Double) extends Command

    sealed trait Event
    case class Added(id:Int, amount: Double) extends Event
    case class Multiplied(id:Int, amount: Double) extends Event
    case class Divided(id:Int, amount: Double) extends Event

    final case class State(value: Double) extends CborSerialization {
      def add(amount: Double): State = copy(value = value + amount)
      def multiply(amount: Double): State = copy(value = value * amount)
      def divide(amount: Double): State = copy(value = value / amount)
    }

    object State {
      val empty = State(0)
    }


    def handleCommand(
                       persistenceId: String,
                       state: State,
                       command: Command,
                       ctx: ActorContext[Command]
                     ): Effect[Event, State] =
      command match {
        case Add(amount) =>
          ctx.log.info(s"receive adding  for number: $amount and state is ${state.value}")
          val added = Added(persistenceId.toInt, amount)
          Effect
            .persist(added)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
        case Multiply(amount) =>
          ctx.log.info(s"receive multiplying  for number: $amount and state is ${state.value}")
          val multiplied = Multiplied(persistenceId.toInt, amount)
          Effect
            .persist(multiplied)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
        case Divide(amount) =>
          ctx.log.info(s"receive dividing  for number: $amount and state is ${state.value}")
          val divided = Divided(persistenceId.toInt, amount)
          Effect
            .persist(divided)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
      }

    def handleEvent(state: State, event: Event, ctx: ActorContext[Command]): State =
      event match {
        case Added(_, amount) =>
          ctx.log.info(s"Handling event Added is: $amount and state is ${state.value}")
          state.add(amount)
        case Multiplied(_, amount) =>
          ctx.log.info(s"Handling event Multiplied is: $amount and state is ${state.value}")
          state.multiply(amount)
        case Divided(_, amount) =>
          ctx.log.info(s"Handling event Divided is: $amount and state is ${state.value}")
          state.divide(amount)
      }

    def apply(): Behavior[Command] =
      Behaviors.setup{ ctx =>
        EventSourcedBehavior[Command, Event, State](
          persistenceId = persId,
          State.empty,
          (state, command) => handleCommand("001", state, command, ctx),
          (state, event) => handleEvent(state, event, ctx)
        )
      }

  }


  case class TypedCalculatorReadSide(system: ActorSystem[NotUsed])(implicit executionContext: ExecutionContextExecutor) {

    implicit val materializer = system.classicSystem
    implicit val session: SlickSession = SlickSession.forDbAndProfile(db, profile)
    var res: Result = getLatestsOffsetAndResultSlick
    val startOffset: Int = if (res.offset == 1) 1 else res.offset + 1

    val readJournal: CassandraReadJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
    RunnableGraph.fromGraph(graph).run()

    /*
    /**
     * В read side приложения с архитектурой CQRS (объект TypedCalculatorReadSide в TypedCalculatorReadAndWriteSide.scala) необходимо разделить бизнес логику и запись в целевой получатель, т.е.
     * 1) Persistence Query должно находиться в Source
     * 2) Обновление состояния необходимо переместить в отдельный от записи в БД флоу
     * 3) ! Задание со звездочкой: вместо CalculatorRepository создать Sink c любой БД (например Postgres из docker-compose файла).
     * Для последнего задания пригодится документация - https://doc.akka.io/docs/alpakka/current/slick.html#using-a-slick-flow-or-sink
     * Результат выполненного д.з. необходимо оформить либо на github gist либо PR к текущему репозиторию.
     *
     * */

    как делать:
    1. в типах int заменить на double
    3. добавить функцию updateState в которой будет паттерн матчинг событий Added Multiplied Divided
    4.создаете graphDsl  в котором: builder.add(source)
    5. builder.add(Flow[EventEnvelope].map( e => updateState(e.event, e.seqNr)))
     */


    val source: Source[EventEnvelope, NotUsed] = readJournal.eventsByPersistenceId("001", startOffset, Long.MaxValue)

    def updateState(event: EventEnvelope): Result = event.event match {
      case Added(_, amount) =>
        val newRes = Result(res.state + amount, event.sequenceNr)
        updatedResultAndOffset(newRes)
        println(s"Log from Added: ${newRes.state}")
        newRes
      case Multiplied(_, amount) =>
        val newRes = Result(res.state * amount, event.sequenceNr)
        updatedResultAndOffset(newRes)
        println(s"Log from Multiplied:  ${newRes.state}")
        newRes
      case Divided(_, amount) =>
        val newRes = Result(res.state / amount, event.sequenceNr)
        updatedResultAndOffset(newRes)
        println(s"Log from Divided:  ${newRes.state}")
        newRes
    }

    val graph: Graph[ClosedShape.type, NotUsed] = GraphDSL.create() {
      implicit builder: GraphDSL.Builder[NotUsed] =>
        //1.
        val input = builder.add(source)
        val stateUpdater = builder.add(Flow[EventEnvelope].map(e => updateState(e)))
        val localSaveOutput = builder.add(Sink.foreach[Result] {
          r =>
            res = res.copy(state = r.state)
            println("something to print")
        })

        val dbSaveOutput = builder.add(
          Slick.sink(updatedResultAndOffsetSlick)
        )
        val broadcast = builder.add(Broadcast[Result](2))

        import GraphDSL.Implicits._
        input ~> stateUpdater ~> broadcast

        broadcast.out(0) ~> dbSaveOutput
        broadcast.out(1) ~> localSaveOutput

        ClosedShape
    }

  }

  object CalculatorRepository{

    val db = Database.forURL("jdbc:postgresql://localhost/demo",
      driver = "org.postgresql.Driver")
    val profile = slick.jdbc.PostgresProfile

    case class Result(state: Double, offset: Long)

    case class ResultTable(tag: Tag) extends Table[(Double, Long)](tag, "RESULT") {
      def state = column[Double]("CALCULATED_VALUE")

      def offset = column[Long]("WRITE_SIDE_OFFSET")

      def * = (state, offset)
    }

    def getLatestsOffsetAndResultSlick(implicit executionContext: ExecutionContextExecutor): Result = {
      val q = sql"""select calculated_value, write_side_offset from public.result where id = 1;""".as[(Double, Long)].headOption
      //надо создать future для db.run
      //с помошью await получите результат или прокиньте ошибку если результат нет
      val f = db.run(q).map(v => v.flatMap(r => Some(Result(r._1, r._2))))
      Await.result(f, 10000.nanos)
    }.getOrElse(throw new RuntimeException("no values in db"))



    def getLatestOffsetAndResult: (Int, Double) ={
      val entities =
        DB readOnly { session=>
          session.list("select * from public.result where id = 1;") {
            row => (
              row.int("write_side_offset"),
              row.double("calculated_value"))
          }
        }
      entities.head
    }


    //homework how to do
    def updatedResultAndOffset(result: Result): Unit ={
      using(DB(ConnectionPool.borrow())) {
        db =>
          db.autoClose(true)
          db.localTx {
            _.update("update public.result set calculated_value = ?, write_side_offset = ? where id = 1"
              , result.state, result.offset)
          }
      }
    }
  }


  private def updatedResultAndOffsetSlick = (res: Result) => sqlu"update public.result set calculated_value = ${res.state}, write_side_offset = ${res.offset} where id = 1"

  def apply(): Behavior[NotUsed] =
    Behaviors.setup{
      ctx =>
        val writeAcorRef = ctx.spawn(TypedCalculatorWriteSide(), "Calc", Props.empty)
        writeAcorRef ! Add(10)
        writeAcorRef ! Multiply(2)
        writeAcorRef ! Divide(5)

        Behaviors.same
    }

  def execute(command: Command): Behavior[NotUsed] =
    Behaviors.setup{ ctx =>
      val writeAcorRef = ctx.spawn(TypedCalculatorWriteSide(), "Calc", Props.empty)
      writeAcorRef ! command
      Behaviors.same
    }

  def main(args: Array[String]): Unit = {
    val value = akka_typed()
    implicit  val system: ActorSystem[NotUsed] = ActorSystem(value, "akka_typed")
    implicit val executionContext: ExecutionContextExecutor = system.executionContext

    TypedCalculatorReadSide(system)
  }

}