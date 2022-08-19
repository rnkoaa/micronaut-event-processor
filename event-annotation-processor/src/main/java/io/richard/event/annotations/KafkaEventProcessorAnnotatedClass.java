package io.richard.event.annotations;

import java.util.List;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;

public class KafkaEventProcessorAnnotatedClass {
    private ExecutableElement element;
    private TypeElement enclosingElement;
    private int parameterCount;
    private String enclosingElementName = "";
    private String eventClassName = "";
    private VariableElement eventClass;

    public static KafkaEventProcessorAnnotatedClass of(ExecutableElement it) {
        var processorCollector = new KafkaEventProcessorAnnotatedClass()
            .withEnclosingElement((TypeElement) it.getEnclosingElement())
            .withElement(it);

        List<? extends VariableElement> parameters = it.getParameters();
        if (parameters == null || parameters.size() == 0) {
            return processorCollector;
        }

        VariableElement variableElement = it.getParameters().get(0);
        return processorCollector
            .withParameterCount(parameters.size())
            .withEventClass(variableElement)
            .withEventClassName(variableElement.asType().toString());
    }

    private KafkaEventProcessorAnnotatedClass withEventClass(VariableElement eventClass) {
        this.eventClass = eventClass;
        return this;
    }

    private KafkaEventProcessorAnnotatedClass withEventClassName(String fullEventClassName) {
        String[] split = fullEventClassName.split("\\.");
        this.eventClassName = split[split.length - 1];
        return this;
    }

    private KafkaEventProcessorAnnotatedClass withEnclosingElement(TypeElement enclosingElement) {
        this.enclosingElement = enclosingElement;
        this.enclosingElementName = enclosingElement.asType().toString();
        return this;
    }

    private KafkaEventProcessorAnnotatedClass withParameterCount(int paramCount) {
        this.parameterCount = paramCount;
        return this;
    }

    private KafkaEventProcessorAnnotatedClass withElement(ExecutableElement it) {
        this.element = it;
        return this;
    }

    public TypeElement getEnclosingElement() {
        return enclosingElement;
    }

    public VariableElement getEventClass() {
        return this.eventClass;
    }

    /**
     * @return Method Name
     */
    public String getElementName() {
        return element.getSimpleName().toString();
    }

    public int getParameterCount() {
        return parameterCount;
    }

    public String getEnclosingElementName() {
        return enclosingElementName;
    }

    public String getEventClassName() {
        return eventClassName;
    }

    public String id() {
        return element.getSimpleName().toString();
    }
}
