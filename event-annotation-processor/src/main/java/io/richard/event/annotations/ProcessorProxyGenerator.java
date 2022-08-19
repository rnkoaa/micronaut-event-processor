package io.richard.event.annotations;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import javax.annotation.processing.Filer;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.VariableElement;

public class ProcessorProxyGenerator {

    private final Filer filer;

    private static final String DELEGATE_FIELD = "delegate";
    private static final String HANDLE_METHOD_NAME = "handle";
    private static final String EVENT_RECORD_DATA_VARIABLE = "eventRecordData";
    private static final String EVENT_RECORD_PARAM = "eventRecord";
    private static final String PARTITION_KEY = "partitionKey";
    private static final String CORRELATION_ID = "correlationId";
    static final String PROXY_IMPL_SUFFIX = "ProxyImpl";

    public ProcessorProxyGenerator(Filer filer) {
        this.filer = filer;
    }

    public void generate(KafkaEventProcessorAnnotatedClass processorCollector) throws IOException {
        String className = String.format("%s%s", processorCollector.getEventClassName(), PROXY_IMPL_SUFFIX);

        TypeName delegateTypeName = TypeName.get(processorCollector.getEnclosingElement().asType());
        TypeSpec.Builder typeSpec = TypeSpec.classBuilder(className)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addSuperinterface(ProcessorProxy.class)
            .addAnnotation(AnnotationSpec.builder(Named.class)
                .addMember("value", "$S", StringUtils.toCamelCase(className))
                .build())
            .addAnnotation(AnnotationSpec.builder(Singleton.class).build())
            .addField(FieldSpec.builder(delegateTypeName, DELEGATE_FIELD)
                .addModifiers(Modifier.PRIVATE, Modifier.FINAL)
                .build())
            .addMethod(generateConstructor(delegateTypeName))
            .addMethod(handleMethod(processorCollector));

        JavaFile javaFile = JavaFile.builder("io.richard.event.processor", typeSpec.build())
            .build();
        javaFile.writeTo(filer);
    }

    private MethodSpec generateConstructor(TypeName delegateTypeName) {

        return MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addParameter(ParameterSpec.builder(delegateTypeName, DELEGATE_FIELD)
                .build())
            .addStatement(
                "this.$L = $L", DELEGATE_FIELD, DELEGATE_FIELD)
            .build();
    }

    private MethodSpec handleMethod(KafkaEventProcessorAnnotatedClass processorCollector) {
        var methodSpecBuilder = MethodSpec.methodBuilder(HANDLE_METHOD_NAME)
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(EventRecord.class, EVENT_RECORD_PARAM);

        VariableElement variableElement = processorCollector.getEventClass();
        methodSpecBuilder.addStatement(
            "var $L = ($T)$L.data()",
            EVENT_RECORD_DATA_VARIABLE,
            TypeName.get(variableElement.asType()),
            EVENT_RECORD_PARAM
        );

        if (processorCollector.getParameterCount() == 3) {
            methodSpecBuilder.addStatement(
                    "var $L = $L.$L()",
                    CORRELATION_ID,
                    EVENT_RECORD_PARAM,
                    CORRELATION_ID
                )
                .addStatement(
                    "var $L = $L.$L()",
                    PARTITION_KEY,
                    EVENT_RECORD_PARAM,
                    PARTITION_KEY
                );
            methodSpecBuilder.addStatement(
                "this.$L.$L($L, $L, $L)",
                DELEGATE_FIELD,
                processorCollector.getElementName(),
                EVENT_RECORD_DATA_VARIABLE,
                CORRELATION_ID,
                PARTITION_KEY
            );
        }

        if (processorCollector.getParameterCount() == 2) {
            methodSpecBuilder.addStatement(
                "var $L = $L.$L()",
                CORRELATION_ID,
                EVENT_RECORD_PARAM,
                CORRELATION_ID
            );
            methodSpecBuilder.addStatement(
                "this.$L.$L($L, $L)",
                DELEGATE_FIELD,
                processorCollector.getElementName(),
                EVENT_RECORD_DATA_VARIABLE,
                CORRELATION_ID
            );
        }

        if (processorCollector.getParameterCount() == 1) {
            methodSpecBuilder.addStatement(
                "this.$L.$L($L)",
                DELEGATE_FIELD,
                processorCollector.getElementName(),
                EVENT_RECORD_DATA_VARIABLE
            );

        }

        return methodSpecBuilder.build();
    }
}
