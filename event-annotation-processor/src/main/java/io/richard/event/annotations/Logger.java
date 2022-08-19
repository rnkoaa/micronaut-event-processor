package io.richard.event.annotations;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.processing.Messager;
import javax.tools.Diagnostic.Kind;

public class Logger {

    private static final Map<Class<?>, Logger> loggers = new ConcurrentHashMap<>();
    private final String cannonicalName;
    private final String simpleName;
    private final Messager messager;

    private Logger(Class<?> clzz, Messager messager) {
        this.cannonicalName = clzz.getCanonicalName();
        this.simpleName = clzz.getSimpleName();
        this.messager = messager;
    }

    public static Logger init(Class<?> clzz, Messager messager) {
//        return loggers.computeIfAbsent(clzz, aClass -> {
//            Logger logger = new Logger(clzz, messager);
//            loggers.put(clzz, logger);
//            return logger;
//        });
//
        Logger logger = loggers.get(clzz);
        if (logger != null) {
            return logger;
        }

        logger = new Logger(clzz, messager);
        loggers.put(clzz, logger);
        return logger;
    }

    public void info(String message) {
        messager.printMessage(Kind.NOTE, String.format("[%s] - %s", simpleName, message));
    }

    public void info(String message, Object... args) {
        String prefix = "[%s] - ".formatted(simpleName);
        String msg = prefix + message;
        messager.printMessage(
            Kind.NOTE,
            String.format(msg, args));
//        messager.printMessage(Kind.NOTE, String.format("[%s] - %s", simpleName, message));
    }

    public void error(String message) {
        messager.printMessage(Kind.ERROR, String.format("[%s] - %s", simpleName, message));
    }


    public void error(String message, Object... args) {
        String prefix = "[%s] - ".formatted(simpleName);
        String msg = prefix + message;
        messager.printMessage(
            Kind.ERROR,
            String.format(msg, args));
//        messager.printMessage(Kind.NOTE, String.format("[%s] - %s", simpleName, message));
    }
    /*
    private void error(String message, Element element) {
        messager.printMessage(Diagnostic.Kind.ERROR, message, element);
    }

    protected void error(String message) {
        messager.printMessage(Diagnostic.Kind.ERROR, message);
    }

    protected void note(String message) {
        messager.printMessage(Diagnostic.Kind.NOTE, message);
    }

    protected void error(Element e, String msg, Object... args) {
        messager.printMessage(
            Diagnostic.Kind.ERROR,
            String.format(msg, args),
            e);
    }

    protected void note(String msg, Object... args) {
        messager.printMessage(
            Kind.NOTE,
            String.format(msg, args));
    }

    protected void error(String msg, Object... args) {
        messager.printMessage(
            Diagnostic.Kind.ERROR,
            String.format(msg, args));
    }
     */
}
