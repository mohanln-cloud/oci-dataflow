package com.mk.logger;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

@Plugin(
        name = "CustomMapAppender",
        category = Core.CATEGORY_NAME,
        elementType = Appender.ELEMENT_TYPE)
public class CustomMapAppender extends AbstractAppender {

    private final FileAppender appenders = getAppender();

    protected CustomMapAppender(String name, Filter filter) {
        super(name, filter, null);
    }

    private FileAppender getAppender(){
        System.out.println("Creating application Custom log appender based on Log4j File Appender.");
        FileAppender fileAppender =  FileAppender.newBuilder().build();

        fileAppender.getImmediateFlush();
//        fileAppender.setName("test");
//        fileAppender.setFile("myApp.log");
//        fileAppender.setThreshold(Level.DEBUG);
//        fileAppender.setAppend(true);
//        PatternLayout consoleLogsPattern = new PatternLayout();
//        // Since the application logs also come from System.out and have no class name etc in log lines.
//        consoleLogsPattern.setConversionPattern("%m%n");
//        fileAppender.setLayout(consoleLogsPattern);
//        fileAppender.activateOptions();
        return fileAppender;
    }

    @PluginFactory
    public static CustomMapAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginElement("Filter") Filter filter) {
        return new CustomMapAppender(name, filter);
    }

    @Override
    public void append(LogEvent event) {
        System.out.print("Inside Custom log" + event.getMessage().getFormattedMessage());

        //this.appenders.doAppend(event);
        //eventMap.put(Instant.now().toString(), event);
    }
}