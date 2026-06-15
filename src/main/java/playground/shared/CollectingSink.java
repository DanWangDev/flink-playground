package playground.shared;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A Flink sink that collects all received records into a synchronized List.
 * Uses the Flink 2.0 Sink/SinkWriter API.
 */
public class CollectingSink<T> implements Sink<T> {

    private static final List<Object> collected = Collections.synchronizedList(new ArrayList<>());

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        return new CollectingWriter<>();
    }

    private static class CollectingWriter<T> implements SinkWriter<T> {

        @Override
        public void write(T element, Context context) throws IOException {
            collected.add(element);
        }

        @Override
        public void flush(boolean endOfInput) {
            // no-op: data is already in the list
        }

        @Override
        public void close() {
            // no-op
        }
    }

    /** Get all collected values (clears the internal list after reading) */
    @SuppressWarnings("unchecked")
    public List<T> getValues() {
        synchronized (collected) {
            List<T> copy = new ArrayList<>((List<T>) (List<?>) collected);
            collected.clear();
            return copy;
        }
    }

    /** Get current size without clearing */
    public int size() {
        return collected.size();
    }

    /** Clear collected values */
    public static void clearAll() {
        collected.clear();
    }
}
