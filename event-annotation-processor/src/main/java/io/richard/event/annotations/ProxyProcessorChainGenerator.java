package io.richard.event.annotations;

import static io.richard.event.annotations.JavaPoetHelpers.classOfAny;
import static io.richard.event.annotations.ProcessorProxyGenerator.PROXY_IMPL_SUFFIX;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.processing.Filer;
import javax.lang.model.element.Modifier;

public class ProxyProcessorChainGenerator {

    private static final String PROXY_PROCESSOR_CLASS = "ProxyProcessorChain";

    void generateProxyProcessorChain(List<KafkaEventProcessorAnnotatedClass> processorCollectors, Filer filer)
        throws IOException {

        String paramDependencyInjectionAdapter = "dependencyInjectionAdapter";
        CodeBlock.Builder staticBlockBuilder = CodeBlock.builder();
        processorCollectors.forEach(processorCollector -> {
            String className = String.format("%s%s", processorCollector.getEventClassName(), PROXY_IMPL_SUFFIX);
            TypeName typeName = TypeName.get(processorCollector.getEventClass().asType());
            staticBlockBuilder.addStatement(
                "proxyProcessors.put($T.class, new $T($L, $L.class))",
                typeName,
                ProcessorHandlerDetails.class,
                processorCollector.getParameterCount(), className);
        });

        TypeSpec.Builder processChainBuilder = TypeSpec.classBuilder(PROXY_PROCESSOR_CLASS)
            .addModifiers(Modifier.FINAL, Modifier.PUBLIC)
            .addAnnotation(AnnotationSpec.builder(Named.class)
                .addMember("value", "$S", StringUtils.toCamelCase(PROXY_PROCESSOR_CLASS))
                .build())
            .addAnnotation(AnnotationSpec.builder(Singleton.class).build())
            .addField(FieldSpec.builder(DependencyInjectionProvider.class, paramDependencyInjectionAdapter)
                .addModifiers(Modifier.FINAL, Modifier.PRIVATE)
                .build())
            .addField(generateProxyProcessorsField("proxyProcessors"))
            .addStaticBlock(staticBlockBuilder.build())
            .addMethod(MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addParameter(ParameterSpec.builder(DependencyInjectionProvider.class, paramDependencyInjectionAdapter)
                    .build())
                .addStatement("this.$L = $L", paramDependencyInjectionAdapter, paramDependencyInjectionAdapter)
                .build())
            .addMethod(generateProcessMethod());

        JavaFile javaFile = JavaFile.builder("io.richard.event.processor", processChainBuilder.build())
            .build();
        javaFile.writeTo(filer);
    }

    MethodSpec generateProcessMethod() {
        MethodSpec.Builder methodSpecBuilder = MethodSpec.methodBuilder("process")
            .addModifiers(Modifier.PUBLIC)
            .addParameter(EventRecord.class, "eventRecord");

        methodSpecBuilder.addStatement("var eventRecordData = eventRecord.data()")
            .addCode(CodeBlock.builder()
                .beginControlFlow("if(eventRecordData == null)")
                .addStatement("return")
                .endControlFlow()
                .build());

        methodSpecBuilder
            .addStatement("var dataClass = eventRecord.data().getClass()")
            .addStatement("var handlerDetails = proxyProcessors.get(dataClass)")
            .beginControlFlow("if(handlerDetails == null)")
            .addStatement("throw new $T(dataClass)", EventProcessorNotFoundException.class)
            .endControlFlow();

        methodSpecBuilder.addStatement(
                "var handlerProxy = dependencyInjectionAdapter.getBean(handlerDetails.handlerProxy())")
            .addCode(CodeBlock.builder()
                .add("var processorProxy = handlerProxy\n")
                .add("\t.map(it -> ($T)it)\n", ProcessorProxy.class)
                .add("\t.orElseThrow(() -> new $T(dataClass));\n", EventHandlerNotFoundException.class)
                .build());

        return methodSpecBuilder.addStatement("\nprocessorProxy.handle(eventRecord)")
            .build();
    }

    public static FieldSpec generateProxyProcessorsField(String fieldName) {
        return FieldSpec.builder(
                ParameterizedTypeName.get(
                    ClassName.get(Map.class),
                    classOfAny(),
                    ClassName.get(ProcessorHandlerDetails.class)
                ),
                fieldName
            )
            .addModifiers(Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
            .initializer(CodeBlock.builder()
                .addStatement("new $T<>()", ConcurrentHashMap.class)
                .build())
            .build();
    }
}
