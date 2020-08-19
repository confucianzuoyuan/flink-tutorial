package chapter5.util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class SmokeLevelSource implements SourceFunction<SmokeLevel> {

    // flag indicating whether source is still running
    private boolean running = true;

    /**
     * Continuously emit one smoke level event per second.
     */
    @Override
    public void run(SourceContext<SmokeLevel> srcCtx) throws Exception {

        // initialize random number generator
        Random rand = new Random();

        // emit data until being canceled
        while (running) {

            if (rand.nextGaussian() > 0.8) {
                // emit a high SmokeLevel
                srcCtx.collect(SmokeLevel.HIGH);
            } else {
                srcCtx.collect(SmokeLevel.LOW);
            }

            // wait for 1 second
            Thread.sleep(1000);
        }
    }

    /**
     * Cancel the emission of smoke level events.
     */
    @Override
    public void cancel() {
        this.running = false;

    }
}
