package logs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogTest {
    private static final LogOutput logOutput = new LogOutput();
    public static void main(String[] args) {
        logOutput.logOutput();
    }
}

class LogOutput {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public void logOutput() {
        logger.info("[Info Log]");
        logger.error("[Error Log]");
    }
}
