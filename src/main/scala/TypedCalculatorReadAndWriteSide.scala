import akka.NotUsed
import akka.actor.typed.{ActorSystem, Behavior, Props}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.stream.alpakka.slick.scaladsl.{Slick, SlickSession}
import akka.stream.scaladsl.{Flow, GraphDSL, Sink, Source}
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings, DB}
import akka_typed.CalculatorRepository.{getlatestOffsetAndResult, initdatabase, updateresultAndOffset}
import scalikejdbc.ConnectionPool.borrow
import scalikejdbc.DB.using
import akka_typed.TypedCalculatorWriteSide.{Add, Added, Command, Divide, Divided, Multiply, Multiplied}

object akka_typed {
  trait CborSerialization
  val persId = PersistenceId.ofUniqueId("001")

  object TypedCalculatorWriteSide {
    sealed trait Command
    case class Add(amount: Int) extends Command
    case class Multiply(amount: Int) extends Command
    case class Divide(amount: Int) extends Command

    sealed trait Event
    case class Added(id: Int, amount: Int) extends Event
    case class Multiplied(id: Int, amount: Int) extends Event
    case class Divided(id: Int, amount: Int) extends Event

    final case class State(value: Int) extends CborSerialization{
      def add(amount: Int): State = copy(value = value + amount)
      def multiply(amount: Int): State = copy(value = value * amount)
      def divide(amount: Int): State = copy(value = value / amount)
    }

    object State{
      val empty=State(0)
    }

    def handleCommand(
                     persId: String,
                     state: State,
                     command: Command,
                     ctx: ActorContext[Command]
                     ): Effect[Event, State] =
      command match {
        case Add(amount) =>
          ctx.log.info(s"receive adding for number: $amount and state is ${state.value} ")
          val added = Added(persId.toInt, amount)
          Effect.persist(added)
          .thenRun{
            x=> ctx.log.info(s"The state result is ${x.value}")
          }
        case Multiply(amount) =>
          ctx.log.info(s"receive multiply for number: $amount and state is ${state.value} ")
          val multiplied = Multiplied(persId.toInt, amount)
          Effect.persist(multiplied)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
        case Divide(amount) =>
          ctx.log.info(s"receive divide for number: $amount and state is ${state.value} ")
          val divided = Divided(persId.toInt, amount)
          Effect.persist(divided)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
      }

    def handleEvent(state: State, event: Event, ctx: ActorContext[Command]): State =
      event match {
        case Added(_, amount) =>
          ctx.log.info(s"Handling Event added is: $amount and state is ${state.value}")
          state.add(amount)
        case Multiplied(_, amount) =>
          ctx.log.info(s"Handling Event multiplied is: $amount and state is ${state.value}")
          state.multiply(amount)
        case Divided(_, amount) =>
          ctx.log.info(s"Handling Event divided is: $amount and state is ${state.value}")
          state.divide(amount)
      }

    def apply(): Behavior[Command] =
      Behaviors.setup{ctx =>
        EventSourcedBehavior[Command, Event, State](
          persistenceId = persId,
          State.empty,
          (state, command) => handleCommand("001", state, command, ctx),
          (state, event) => handleEvent(state, event, ctx)
        )
      }

  }


  case class TypedCalculatorReadSide(system: ActorSystem[NotUsed]){
    initdatabase
    implicit val materializer = system.classicSystem
    var (offset, latestCalculatedResult) = getlatestOffsetAndResult
    val startOffset: Int = if (offset == 1) 1 else offset + 1
    val readJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

    /*
    В read side приложения с архитектурой CQRS (объект TypedCalculatorReadSide в TypedCalculatorReadAndWriteSide.scala) необходимо разделить чтение событий, бизнес логику и запись в целевой получатель и сделать их асинхронными, т.е.
    1) Persistence Query должно находиться в Source
    2) Обновление состояния необходимо переместить в отдельный от записи в БД flow, замкнуть пуcтым sink

    Как делать!!!
    1. в типах int заменить на double
    2. добавить функцию updateState, в которой сделать паттерн матчинг для событий Added, Multiplied, Divided
    3. нужно создать graphDsl в котором builder.add(source)
    4. builder.add(Flow[EventEnvelope].map(e=> updateState(e.event, e.sequenceNr)))

     */

    /*
    spoiler
    def updateState(event: Any, seqNum:Long) : Result = {
    val newState = event match {
    case Added(_, amount) =>???
    case Multiplied(_, amount) =>???
    case Divided(_, amount) =>???

    val graph = GraphDSL.Builder[NotUsed] =>
    //1.
    val input = builder.add(source)
    val stateUpdate = builder.add(Flow[EventEnvelope].map(e=> updateState(...)))
    val localSaveOutput = builder.add(Sink.foreach[Result]{
    r=>
    lCr = r.state
    //logs

    })

    val dbSaveOutput = builder.add(
    Sink.sink[Result](r=>updateresultAndOffset)
    )
    надо разделить builder на 2 части
    далее надо сохранить flow(уже разделен на 2 части) в 1. localSaveOutput 2. dbSaveOutput
    закрываем граф и запускаем

     */

    val source: Source[EventEnvelope, NotUsed] = readJournal.eventsByPersistenceId("001", startOffset, Long.MaxValue)

    source
      .map { x =>
        println(x.toString())
        x
      }
      .runForeach{
        event =>
          event.event match {
            case Added(_, amount) =>
              latestCalculatedResult += amount
              updateresultAndOffset(latestCalculatedResult, event.sequenceNr)
              println(s"Log from Added: $latestCalculatedResult")
            case Multiplied(_, amount) =>
              latestCalculatedResult *= amount
              updateresultAndOffset(latestCalculatedResult, event.sequenceNr)
              println(s"Log from Multiplied: $latestCalculatedResult")
            case Divided(_, amount) =>
              latestCalculatedResult /= amount
              updateresultAndOffset(latestCalculatedResult, event.sequenceNr)
              println(s"Log from Divided: $latestCalculatedResult")
          }
      }

  }

  object CalculatorRepository {
    //homework, how to do
    //1. SlickSession здесь надо посмотреть документацию
    //def createSession(): SlickSession


    def initdatabase: Unit = {
      Class.forName("org.postgresql.Driver")
      val poolSettings = ConnectionPoolSettings(initialSize = 10, maxSize = 100)
      ConnectionPool.singleton("jdbc:postgresql://localhost:5432/demo", "docker", "docker", poolSettings)
    }

    // homework
    //case class Result(state: Double, offset: Long)
    // надо переделать getlatestOffsetAndResult
    // def getlatestOffsetAndResult: Result = {
    // val query = sql"select * from public.result where id = 1;".as[Double].headOption
    // надо будет создать future для db.run
    // с помощью await надо получить результат или прокинуть ошибку если результата нет

    def getlatestOffsetAndResult: (Int, Double) = {
      val entities =
        DB readOnly { session =>
          session.list("select * from public.result where id = 1;") {
            row =>
              (
                row.int("write_side_offset"),
                row.double("calculated_value"))
          }
        }
      entities.head
    }

    def updateresultAndOffset(calculated: Double, offset: Long): Unit = {
      using(DB(borrow(ConnectionPool))) {
        db =>
          db.autoClose(true)
          db.localTx{
            _.update("update public.result set calculated_value = ?, write_side_offset = ? where id = 1",
              calculated, offset)
          }
      }
    }
  }

  def apply(): Behavior[NotUsed] =
    Behaviors.setup{
      ctx =>
        val writeActorRef = ctx.spawn(TypedCalculatorWriteSide(), "Calc", Props.empty)
        writeActorRef ! Add(10)
        writeActorRef ! Multiply(2)
        writeActorRef ! Divide(5)
        Behaviors.same
    }

  def main(args: Array[String]): Unit = {
    val value = akka_typed()
    implicit val system: ActorSystem[NotUsed] = ActorSystem(value, "akka_typed")
    implicit  val executionContext = system.executionContext

    TypedCalculatorReadSide(system)
  }


}