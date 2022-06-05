package io.richard.event.annotations;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Elements;

@AutoService(Processor.class)
@SupportedSourceVersion(SourceVersion.RELEASE_17)
public class KafkaEventAnnotationProcessor extends AbstractAnnotationProcessor {

    private Elements elementUtils;
    private Filer filer;
    private EventProcessorGroupFactoryGenerator groupFactoryGenerator;

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        elementUtils = processingEnv.getElementUtils();
        filer = processingEnv.getFiler();
        groupFactoryGenerator = new EventProcessorGroupFactoryGenerator();
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        Set<? extends Element> elementsAnnotatedWith = roundEnv.getElementsAnnotatedWith(KafkaEventProcessor.class);
        if (elementsAnnotatedWith.size() == 0) {
            return false;
        }
        List<? extends Element> invalidElements = elementsAnnotatedWith.stream()
            .filter(it -> !it.getKind().equals(ElementKind.CLASS))
            .toList();

        if (invalidElements.size() > 0) {
            invalidElements.forEach(element -> {
                error(element, "Only classes can be annotated with @KafkaEventProcessor");
            });
            return true;
        }

        elementsAnnotatedWith
            .stream()
            .map(it -> (TypeElement) it)
            .forEach(it -> {
                try {
                    groupFactoryGenerator.add(new KafkaEventAnnotatedClass(it));
                } catch (ProcessingException e) {
                    e.printStackTrace();
                }
            });

        try {
            groupFactoryGenerator.generateGroupFactoryClass(elementUtils, filer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return false;
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Collections.singleton(KafkaEventProcessor.class.getCanonicalName());
    }
}
