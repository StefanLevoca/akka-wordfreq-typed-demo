package sk.upjs.ics.kopr;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

public class WordFrequencyCounter extends AbstractBehavior<WordFrequencyCounter.GetWordFrequencies> {

    public static Behavior<GetWordFrequencies> create() {
        return Behaviors.setup(WordFrequencyCounter::new);
    }

    private WordFrequencyCounter(ActorContext<GetWordFrequencies> context) {
        super(context);
    }

    @Override
    public Receive<GetWordFrequencies> createReceive() {
        return newReceiveBuilder()
                .onMessage(GetWordFrequencies.class, this::getWordFrequencies)
                .build();
    }

    private Behavior<GetWordFrequencies> getWordFrequencies(GetWordFrequencies command) {
        String sentence = command.getSentence();
        Map<String, Long> frequencies = Stream.of(sentence.split("\\s"))
                .collect(groupingBy(String::toString, counting()));

        getContext().getLog().info("Frequencies: {}", frequencies);

        WordFrequenciesCalculated event = new WordFrequenciesCalculated(frequencies);
        command.replyTo.tell(event);

        return this;
    }

    public static class GetWordFrequencies {
        private final String sentence;

        private final ActorRef<WordFrequenciesCalculated> replyTo;

        public GetWordFrequencies(String sentence,
                                  ActorRef<WordFrequenciesCalculated> replyTo) {
            this.sentence = sentence;
            this.replyTo = replyTo;
        }

        public String getSentence() {
            return sentence;
        }
    }

    public static class WordFrequenciesCalculated {
        private final Map<String, Long> frequencies;

        public WordFrequenciesCalculated(Map<String, Long> frequencies) {
            this.frequencies = frequencies;
        }

        public Map<String, Long> getFrequencies() {
            return frequencies;
        }
    }


    public static void main(String[] args) {
        ActorSystem<GetWordFrequencies> system = ActorSystem.create(WordFrequencyCounter.create(), "system");

        CompletionStage<WordFrequenciesCalculated> result = AskPattern.ask(system,
                replyTo -> new GetWordFrequencies("Life is Life", replyTo),
                Duration.ofSeconds(5),
                system.scheduler()
        );
        result.whenComplete((wordFrequenciesCalculated, throwable) -> {
            System.out.println("Total frequencies: "
                    + wordFrequenciesCalculated.getFrequencies());
        });
    }
}
