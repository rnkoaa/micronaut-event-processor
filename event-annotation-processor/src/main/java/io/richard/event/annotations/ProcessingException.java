package io.richard.event.annotations;

import java.io.IOException;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;

public class ProcessingException extends Exception {

    Element element;

    public ProcessingException(Element element, String msg, Object... args) {
        super(String.format(msg, args));
        this.element = element;
    }

    public ProcessingException(IOException ex) {
        super(ex);
    }

    public Element getElement() {
        return element;
    }
}