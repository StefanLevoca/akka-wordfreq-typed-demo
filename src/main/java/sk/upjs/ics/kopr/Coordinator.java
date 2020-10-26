package sk.upjs.ics.kopr;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

import java.util.Map;

public class Coordinator extends AbstractBehavior<Coordinator.Command> {
    private ActorRef<WordFrequencyCounter.WordFrequenciesCalculated> messageAdapter;

    private ActorRef<WordFrequencyCounter.GetWordFrequencies> wordFrequencyCounter;

    public static Behavior<Command> create() {
        return Behaviors.setup(Coordinator::new);
    }

    private Coordinator(ActorContext<Command> context) {
        super(context);

        this.wordFrequencyCounter = context.spawn(
                WordFrequencyCounter.create(),
                "worker");

        this.messageAdapter = context.messageAdapter(
                WordFrequencyCounter.WordFrequenciesCalculated.class,
                event -> new Coordinator.AggregateWordFrequencies(event.getFrequencies())
        );
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(GetWordFrequencies.class, this::getWordFrequencies)
                .onMessage(AggregateWordFrequencies.class, this::aggregateWordFrequencies)
                .build();
    }


    private Behavior<Command> aggregateWordFrequencies(AggregateWordFrequencies command) {
        getContext().getLog().info("Aggregated frequencies: {}", command.getFrequencies());

        return this;
    }

    private Behavior<Command> getWordFrequencies(GetWordFrequencies command) {
        WordFrequencyCounter.GetWordFrequencies delegatedCommand
                = new WordFrequencyCounter.GetWordFrequencies(
                        command.getSentence(),
                        this.messageAdapter
                );

        getContext().getLog().info("Delegating sentence to Word Frequency Counter");

        this.wordFrequencyCounter.tell(delegatedCommand);

        return this;
    }

    public interface Command {}

    public static class GetWordFrequencies implements Command {
        private final String sentence;

        public GetWordFrequencies(String sentence) {
            this.sentence = sentence;
        }

        public String getSentence() {
            return sentence;
        }
    }

    public static class AggregateWordFrequencies implements Command {
        private final Map<String, Long> frequencies;


        public AggregateWordFrequencies(Map<String, Long> frequencies) {
            this.frequencies = frequencies;
        }

        public Map<String, Long> getFrequencies() {
            return frequencies;
        }
    }

    public static void main(String[] args) {
        ActorSystem<Command> system = ActorSystem.create(Coordinator.create(), "system");
        system.tell(new GetWordFrequencies("Life is Life"));
        system.tell(new GetWordFrequencies("Dog eat Dog"));
    }
}
