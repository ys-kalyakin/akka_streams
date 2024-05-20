package TypedCalculatorReadAndWriteSide

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings, DB, using}

object akka_typed {
  val persId = PersistenceId.ofUniqueId("001")

  object TypedCalculationWriteSide {
    sealed trait Command
    case class Add(amount: Int) extends  Command
    case class Multiply(amount: Int) extends Command
    case class Divide(amount: Int) extends Command

    sealed trait Event
    case class Added(id: Int, amount: Int) extends Event
    case class Multiplied(id: Int, amount: Int) extends Event
    case class Divided(id: Int, amount: Int) extends Event


    final case class State(value: Int)  {
      def add(amount: Int) : State = copy(value = value+amount)
      def multiply(amount:Int): State = copy(value=value*amount)
      def divide(amount: Int): State = copy(value=value/amount)
    }

    object State{
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
          ctx.log.info(s"receive adding for number: $amount and state is ${state.value}")
          val added = Added(persistenceId.toInt, amount)
          Effect
          .persist(added)
          .thenRun{
            x=> ctx.log.info(s"The state result is ${x.value}")
          }
        case Multiply(amount) =>
          ctx.log.info(s"receive multiplying for number: $amount and state is ${state.value}")
          val multiplied = Multiplied(persistenceId.toInt, amount)
          Effect
            .persist(multiplied)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
        case Divide(amount) =>
          ctx.log.info(s"receive dividing for number: $amount and state is ${state.value}")
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
      Behaviors.setup{ctx =>
        EventSourcedBehavior[Command, Event, State](
          persistenceId = persId,
          State.empty,
          (state, command) => handleCommand("001", state, command, ctx),
          (state, event) => handleEvent(state, event, ctx)
        )
      }

  }

  object CalculatorRepository {
    def initDatabase: Unit = {
      Class.forName("org.postgres.Driver")
      val poolSettings = ConnectionPoolSettings(initialSize = 10, maxSize = 100)
      ConnectionPool.singleton("jdbc:postgresql://localhost:5432/demo", "docker", "docke", poolSettings)
    }


    def getLatestOffsetAndResult: (Int, Double) ={
      val entities = {
        DB readOnly { session =>
          session.list("select * from public.result where id = 1;") {
            row => (
              row.int("write_side_offset"),
              row.double("calculated_value")
            )
          }
        }
      }

      entities.head
    }

    def updateResultAndOffset(calculated: Double, offset: Long): Unit = {
      using(DB(ConnectionPool.borrow())) {
        db =>
        db.autoClose(true)
          db.localTx{
            _.update("update public.result set calculated_value = ?, write_side_offset=? where id =1", calculated, offset)
          }
      }
    }

  }
}