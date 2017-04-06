package sexy.kome.hadoop.weather.generator;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Hack on 2016/11/20.
 */
public class WeatherLogger {
    private static final Logger RUN_LOG = Logger.getLogger(WeatherLogger.class);
    private static final BlockingQueue<WeatherRecord> QUEUE = new ArrayBlockingQueue<WeatherRecord>(100);
    private static final String DEFAULT_LOG_FILE = WeatherLogger.class.getClassLoader().getResource(".").getPath().concat("hadoop/weather/weather.log");
    private static final Thread WRITE_LOG_THREAD = new WriteLogThread();
    static {
        File logFile = new File(DEFAULT_LOG_FILE);
        try {
            if (logFile.exists()) {
                logFile.delete();
            }
            logFile.getParentFile().mkdirs();
            logFile.createNewFile();
        } catch (Exception e) {
            RUN_LOG.error(e.getMessage(), e);
        }
    }

    private WeatherLogger() {
    }

    public static final void log(WeatherRecord record) {
        QUEUE.offer(record);
        System.out.println("++" + Thread.currentThread().getId()+ " " + QUEUE.size());

        if (!WRITE_LOG_THREAD.isAlive()) {
            WRITE_LOG_THREAD.start();
        }
    }

    private static class WriteLogThread extends Thread {
        @Override
        public void run() {
            try (FileWriter writer = new FileWriter(DEFAULT_LOG_FILE, true)) {
                while (!Thread.currentThread().isInterrupted()) {
                    writer.write(QUEUE.take().toString().concat("\n"));
                    System.out.println("--" + Thread.currentThread().getId()+ " " + QUEUE.size());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void stopWrite() {
        while (!QUEUE.isEmpty()) {
            try {
                Thread.sleep(1000);
                System.out.println("Waiting for consume WeatherLog - " + QUEUE.size());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (!WRITE_LOG_THREAD.isInterrupted()) {
            WRITE_LOG_THREAD.interrupt();
        }
    }
}
