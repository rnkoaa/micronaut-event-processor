package io.richard.event.annotations;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Types;

@AutoService(Processor.class)
@SupportedSourceVersion(SourceVersion.RELEASE_17)
public class KafkaEventAnnotationProcessor extends AbstractAnnotationProcessor {

    private Logger logger;
    Messager messager;
    Types typeUtils;
    Filer filer;
    private ProcessorProxyGenerator processorProxyGenerator;
    private ProxyProcessorChainGenerator proxyProcessorChainGenerator;

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        messager = processingEnv.getMessager();
        typeUtils = processingEnv.getTypeUtils();
        filer = processingEnv.getFiler();
        logger = Logger.init(KafkaEventAnnotationProcessor.class, messager);
        processorProxyGenerator = new ProcessorProxyGenerator(filer);
        proxyProcessorChainGenerator = new ProxyProcessorChainGenerator();
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        Set<? extends Element> kafkaEventProcessors = roundEnv.getElementsAnnotatedWith(KafkaEventProcessor.class);
        if (kafkaEventProcessors.size() == 0) {
            logger.info("Found no Kafka Event Processors on classpath");
            return true;
        }
        List<? extends Element> invalidElements = kafkaEventProcessors.stream()
            .filter(it -> !it.getKind().equals(ElementKind.METHOD))
            .toList();

        if (invalidElements.size() > 0) {
            invalidElements.forEach(element -> {
                error(element, "Only classes can be annotated with @KafkaEventProcessor");
            });
            return true;
        }

        List<KafkaEventProcessorAnnotatedClass> processorCollectors = kafkaEventProcessors.stream()
            .map(it -> (ExecutableElement) it)
            .map(KafkaEventProcessorAnnotatedClass::of)
            .toList();

        // ensure each processor has at least one param
        List<KafkaEventProcessorAnnotatedClass> nullOrEmptyParamMethods = processorCollectors.stream()
            .filter(it -> it.getParameterCount() == 0)
            .toList();

        if (nullOrEmptyParamMethods.size() > 0) {
            logger.error("Found %d methods with null or 0 params", nullOrEmptyParamMethods.size());
        }

        Map<String, List<KafkaEventProcessorAnnotatedClass>> collect = processorCollectors.stream()
            .collect(Collectors.groupingBy(KafkaEventProcessorAnnotatedClass::getEnclosingElementName));
        collect.keySet()
            .forEach(it -> {
                try {
                    List<KafkaEventProcessorAnnotatedClass> processorCollectors1 = collect.get(it);
                    processorProxyGenerator.generate(processorCollectors1.get(0));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

        try {
            proxyProcessorChainGenerator.generateProxyProcessorChain(processorCollectors, filer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Collections.singleton(KafkaEventProcessor.class.getCanonicalName());
    }
}
