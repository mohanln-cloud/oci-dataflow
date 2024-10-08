package com.mk.logger;


import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mk.utils.Constants.*;

@Plugin(
        name = "DataFlowDefaultLogAppender",
        category = "Core",
        elementType = "appender",
        printObject = true
)
public class DataFlowDefaultLogAppender extends AbstractAppender {

    private static final String OPC_REQUEST_ID;
    private static List<String> applicationLogPaths;

    static {
    OPC_REQUEST_ID = System.getProperty("sss.kubernetes.opc-request-id");
    }

        @PluginFactory
        public static DataFlowDefaultLogAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginAttribute("fileName") String fileName,
            @PluginElement("Filter") Filter filter) {
        return new DataFlowDefaultLogAppender(name, filter);
    }

  private DataFlowDefaultLogAppender(String name, Filter filter) {
        super(name, filter, null, false, null);
        applicationLogPaths = new ArrayList<>();
        // The file appender will send logs to application logs file.
        System.out.println("Creating application log appender based on Log4j File Appender.");
        PatternLayout consoleLogsPattern = PatternLayout.createDefaultLayout();

        FileAppender fileAppender = FileAppender.newBuilder()
                .setName(APPLICATION_LOG_APPENDER)
                .withFileName(LOCAL_SPARK_STDOUT_LOG_PATH)
                .setLayout(consoleLogsPattern)
                .withAppend(true)
                .build();

        //fileAppender.setThreshold(Level.DEBUG);
        //fileAppender.setAppend(true);
        //PatternLayout consoleLogsPattern = new PatternLayout();
        // Since the application logs also come from System.out and have no class name etc in log lines.

        //fileAppender.setLayout(consoleLogsPattern);
        //fileAppender.activateOptions();
        this.addAppender(fileAppender);

        // The console appender will send logs to driver and executor stderr logs file.
        System.out.println("Creating diagnostic log appender based on Log4j Console Appender.");
        PatternLayout diagnosticLogsPattern = PatternLayout.createDefaultLayout(); //TO DO : change the pattern
        ConsoleAppender consoleAppender = ConsoleAppender.newBuilder()
                .setTarget(ConsoleAppender.Target.SYSTEM_OUT)
                .setImmediateFlush(true)
                .setLayout(diagnosticLogsPattern)
                .setName(DIAGNOSTIC_LOG_APPENDER).build();


        //diagnosticLogsPattern.setConversionPattern("%d{yy/MM/dd HH:mm:ss} %p [" + OPC_REQUEST_ID + "] %c{1}: %m%n");
        //consoleAppender.setLayout(diagnosticLogsPattern);
        //consoleAppender.activateOptions();
        this.addAppender(consoleAppender);

        System.out.println("Successfully Initialized all appenders in DataFlowDefaultLogAppender!");
    }

        private final List<Appender> appenders = new ArrayList<>();

        public void addAppender(Appender appender) {
        appenders.add(appender);
    }

        // IS_SPARK_APPLICATION_RUNNING is true: any logs from logger should go to application_logs.
        @Override
        public void append(LogEvent event) {
        String isSparkAppRunning = System.getProperty(IS_SPARK_APPLICATION_RUNNING, "true");

        if("true".equals(isSparkAppRunning) && isApplicationLogEvent(event)) {
            this.getAppender(DIAGNOSTIC_LOG_APPENDER).append(event);
        } else {
            this.getAppender(APPLICATION_LOG_APPENDER).append(event);
        }
    }

        private boolean isApplicationLogEvent(LogEvent event) {
        boolean isApplicationLog = false;
        String requestLoggerName = event.getLoggerName();
        setApplicationLogPaths();
        for (String path: applicationLogPaths) {
            //      requestLoggerName = com.user.apps.LogsApplication
            //      path = com.user.apps
            if(requestLoggerName.startsWith(path)) {
                isApplicationLog = true;
                break;
            }
        }
        return isApplicationLog;
    }

        private void setApplicationLogPaths() {
        if(applicationLogPaths.isEmpty()) {
            String pathsProvided = System.getProperty(USER_APPLICATION_LOG_PATHS, "");
            if(!pathsProvided.isEmpty()) {
                applicationLogPaths = Arrays.asList(pathsProvided.split(","));
                for (String path: applicationLogPaths) {
                    System.out.println("Logs from logger utility would be sent to application logs from path: " + path);
                }
            }
        }
    }



        public Appender getAppender(String name) {
        synchronized (appenders) {
            for (Appender appender : appenders) {
                if (appender.getName().equals(name)) {
                    return appender;
                }
            }
        }
        return null;
    }


}

