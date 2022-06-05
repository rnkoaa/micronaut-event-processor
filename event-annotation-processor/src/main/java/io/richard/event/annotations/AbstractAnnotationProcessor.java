package io.richard.event.annotations;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.List;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.util.ElementFilter;
import javax.tools.Diagnostic;
import javax.tools.Diagnostic.Kind;
import javax.tools.FileObject;
import javax.tools.StandardLocation;

public abstract class AbstractAnnotationProcessor extends AbstractProcessor {
    protected String basePackageName = "com.excalibur";

    private Filer filer;
    private Messager messager;

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        this.filer = processingEnv.getFiler();
        this.messager = processingEnv.getMessager();
    }

    private void checkForNoArgumentConstructor(TypeElement type) {
        for (ExecutableElement constructor :
            ElementFilter.constructorsIn(type.getEnclosedElements())) {
            List<? extends VariableElement> constructorParameters =
                constructor.getParameters();
            if (constructor.getParameters().isEmpty()) {
                return;
            }
        }
    }

    protected PackageElement getPackage(Element typeElement) {
        Element element = typeElement;
        while (element != null) {
            if (element instanceof PackageElement) {
                return (PackageElement) element;
            }
            element = element.getEnclosingElement();
        }
        return null;
    }

    protected static void readOldIndexFile(Set<String> entries, Reader reader) throws IOException {
        try (BufferedReader bufferedReader = new BufferedReader(reader)) {
            String line = bufferedReader.readLine();
            while (line != null) {
                entries.add(line);
                line = bufferedReader.readLine();
            }
        }
    }

    protected void writeIndexFile(Set<String> entries, String resourceName, FileObject overrideFile)
        throws IOException {
        FileObject file = overrideFile;
        if (file == null) {
            file = filer.createResource(StandardLocation.CLASS_OUTPUT, "", resourceName);
        }
        try (Writer writer = file.openWriter()) {
            for (String entry : entries) {
                writer.write(entry);
                writer.write("\n");
            }
        }
    }

    protected FileObject readOldIndexFile(Set<String> entries, String resourceName) throws IOException {
        Reader reader = null;
        try {
            final FileObject resource = filer.getResource(StandardLocation.CLASS_OUTPUT, "", resourceName);
            reader = resource.openReader(true);
            readOldIndexFile(entries, reader);
            return resource;
        } catch (FileNotFoundException e) {
            // Ugly hack for Intellij IDEA incremental compilation.
            // The problem is that it throws FileNotFoundException on the files, if they were not created during the
            // current session of compilation.
            final String realPath = e.getMessage();
            if (new File(realPath).exists()) {
                try (Reader fileReader = new FileReader(realPath)) {
                    readOldIndexFile(entries, fileReader);
                }
            }
        } catch (IOException e) {
            // Thrown by Eclipse JDT when not found
        } catch (UnsupportedOperationException e) {
            // Java6 does not support reading old index files
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        return null;
    }

    public boolean hasPublicModifier(Set<Modifier> modifiers) {
        return modifiers.stream()
            .anyMatch(m -> m.equals(Modifier.PUBLIC));
    }

    protected void writeSimpleNameIndexFile(Set<String> elementList, String resourceName)
        throws IOException {

        FileObject file = readOldIndexFile(elementList, resourceName);
        if (file != null) {
//             * Ugly hack for Eclipse JDT incremental compilation.
//             * Eclipse JDT can't createResource() after successful getResource().
//             * But we can file.openWriter().
            try {
                writeIndexFile(elementList, resourceName, file);
                return;
            } catch (IllegalStateException e) {
                // Thrown by HotSpot Java Compiler
            }
        }
        writeIndexFile(elementList, resourceName, null);
    }

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

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latestSupported();
    }
}