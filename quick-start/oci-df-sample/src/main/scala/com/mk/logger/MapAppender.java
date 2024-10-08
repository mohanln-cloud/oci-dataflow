package com.mk.logger;

import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.Serializable;

@Plugin(
        name = "MapAppender",
        category = Core.CATEGORY_NAME,
        elementType = Appender.ELEMENT_TYPE)
public class MapAppender extends AbstractAppender {

    protected MapAppender(String name, Filter filter) {
        super(name, filter, null);
    }

    private FileAppender getAppender(){
        System.out.println("Creating application log appender based on Log4j File Appender.");
        FileAppender fileAppender =  FileAppender.newBuilder().build();
        return fileAppender;
    }

    @PluginFactory
    public static MapAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginElement("Filter") final Filter filter,
            @PluginAttribute("otherAttribute") String otherAttribute) {

            if (name == null) {
                LOGGER.error("No name provided for MyCustomAppenderImpl");
                return null;
            }
            if (layout == null) {
                layout = PatternLayout.createDefaultLayout();
            }
        return new MapAppender(name, filter);
    }

    @Override
    public void append(LogEvent event) {
        System.out.println("Inside log" + event.getMessage().getFormattedMessage());

    }
}