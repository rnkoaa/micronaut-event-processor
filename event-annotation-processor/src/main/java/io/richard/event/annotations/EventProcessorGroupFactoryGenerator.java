package io.richard.event.annotations;

import static javax.lang.model.element.Modifier.PUBLIC;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.context.event.BeanCreatedEventListener;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.processing.Filer;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Elements;

public class EventProcessorGroupFactoryGenerator {

    private static final String CONTEXT_FIELD_NAME = "context";
    private final Map<String, KafkaEventAnnotatedClass> itemsMap = new LinkedHashMap<>();
    private static final String PACKAGE_NAME = "io.richard.event.processor";
    private static final String CLASS_NAME = "EventProcessorGroupFactory";
    private final String kafkaEventProcessorCanonicalName;

    public EventProcessorGroupFactoryGenerator() {
        kafkaEventProcessorCanonicalName = KafkaEventProcessor.class.getCanonicalName().intern();
    }

    public void add(KafkaEventAnnotatedClass toInsert) throws ProcessingException {
        KafkaEventAnnotatedClass existing = itemsMap.get(toInsert.id());
        if (existing != null) {

            // Already existing
            throw new ProcessingException(toInsert.element(),
                "Conflict: The class %s is annotated with @%s with id ='%s' but %s already uses the same id",
                toInsert.element().getQualifiedName().toString(),
                KafkaEventProcessor.class.getSimpleName(),
                toInsert.id(), existing.element().getQualifiedName().toString());
        }

        itemsMap.put(toInsert.id(), toInsert);
    }

    public int size() {
        return itemsMap.size();
    }

    public void clear() {
        itemsMap.clear();
    }

    void generateGroupFactoryClass(Elements elementUtils, Filer filer)
        throws IOException {

        Map<ClassName, KafkaEventAnnotatedClass> annotatedEventByEvent = itemsMap.values()
            .stream()
            .collect(Collectors.toMap(annotatedClass -> getKafkaEventClassName(elementUtils, annotatedClass),
                annotatedClass -> annotatedClass))
            .entrySet()
            .stream()
            .filter(entry -> entry.getKey().isPresent())
            .collect(Collectors.toMap(it -> it.getKey().get(), Entry::getValue));

        TypeSpec.Builder typeSpecBuilder = TypeSpec.classBuilder(CLASS_NAME)
            .addModifiers(PUBLIC, Modifier.FINAL)
            .addAnnotation(Singleton.class)
            .addAnnotation(AnnotationSpec.builder(Named.class)
                .addMember("value", "$S", beanName())
                .build()
            )
            .addSuperinterface(getParameterizedBeanCreatedEventListener())
            .addField(FieldSpec.builder(
                    ApplicationContext.class, CONTEXT_FIELD_NAME, Modifier.PRIVATE, Modifier.FINAL
                ).build()
            )
            .addMethod(generateConstructor())
            .addMethod(generateOnCreatedMethod(annotatedEventByEvent));

        JavaFile javaFile = JavaFile.builder(PACKAGE_NAME, typeSpecBuilder.build())
            .build();
        javaFile.writeTo(filer);
    }

    private Optional<ClassName> getKafkaEventClassName(Elements elementUtils, KafkaEventAnnotatedClass annotatedClass) {
        TypeElement element = annotatedClass.element();
        List<? extends AnnotationMirror> allAnnotationMirrors = elementUtils.getAllAnnotationMirrors(element);
        Optional<? extends AnnotationMirror> kafkaAnnotationMirror = allAnnotationMirrors.stream()
            .filter(this::isKafkaEventProcessor)
            .findFirst();

        if (kafkaAnnotationMirror.isEmpty()) {
            return Optional.empty();
        }

        AnnotationMirror annotationMirror = kafkaAnnotationMirror.get();
        return getAnnotationValue(annotationMirror, "value");
    }

    private boolean isKafkaEventProcessor(AnnotationMirror annotationMirror) {
        TypeElement annotationTypeElement = (TypeElement) annotationMirror.getAnnotationType()
            .asElement();
        return annotationTypeElement.getQualifiedName().toString().equals(kafkaEventProcessorCanonicalName);
    }

    private MethodSpec generateConstructor() {
        return MethodSpec.constructorBuilder()
            .addModifiers(PUBLIC)
            .addParameter(ApplicationContext.class, CONTEXT_FIELD_NAME)
            .addStatement("this.$L = $L", CONTEXT_FIELD_NAME, CONTEXT_FIELD_NAME)
            .build();
    }

    /**
     * Gets the value of a named parameter on the specified annotation or {@code null} if the parameter is not set.
     */
    private static Optional<ClassName> getAnnotationValue(AnnotationMirror annotation, String valueName) {
        // Get all of the values on that annotation
        Map<ExecutableElement, AnnotationValue> vals = Map.copyOf(annotation.getElementValues());
        // Find the value with the name specified
        for (Map.Entry<ExecutableElement, AnnotationValue> entry : vals.entrySet()) {
            if (entry.getKey().getSimpleName().contentEquals(valueName)) {
                ClassName className = ClassName.bestGuess(entry.getValue().getValue().toString());
                return Optional.ofNullable(className);
            }
        }
        return Optional.empty();
    }

    private MethodSpec generateOnCreatedMethod(Map<ClassName, KafkaEventAnnotatedClass> annotatedEventByEvent) {
        String eventProcessorGroupField = "eventProcessorGroup";
        MethodSpec.Builder builder = MethodSpec.methodBuilder("onCreated")
            .addModifiers(PUBLIC)
            .addAnnotation(Override.class)
            .returns(EventProcessorGroup.class)
            .addParameter(ParameterSpec.builder(getParameterizedBeanCreatedEvent(), "event").build())
            .addStatement("\tvar $L = event.getBean()", eventProcessorGroupField);

        annotatedEventByEvent.forEach((className, kafkaEventAnnotatedClass) -> {
            builder.addStatement(
                "\t$L.put($T.class, $L.getBean($T.class))",
                eventProcessorGroupField,
                className,
                CONTEXT_FIELD_NAME, kafkaEventAnnotatedClass.element()
            );
        });

        return builder
            .addStatement("\treturn eventProcessorGroup")
            .build();
    }

    private String beanName() {
        return StringUtils.toCamelCase(CLASS_NAME);
    }

    public static ParameterizedTypeName getParameterizedBeanCreatedEventListener() {
        return ParameterizedTypeName.get(ClassName.get(BeanCreatedEventListener.class),
            ClassName.get(EventProcessorGroup.class));
    }

    public static ParameterizedTypeName getParameterizedBeanCreatedEvent() {
        return ParameterizedTypeName.get(ClassName.get(BeanCreatedEvent.class),
            ClassName.get(EventProcessorGroup.class));
    }
}
