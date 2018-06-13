/**********************************************************************
Copyright (c) 2010 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
   ...
**********************************************************************/
package org.datanucleus.jdo.query;

import java.io.IOException;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.jdo.annotations.NotPersistent;
import javax.jdo.annotations.PersistenceCapable;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.TypeParameterElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.type.TypeVariable;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic.Kind;
import javax.tools.JavaFileObject;

import org.datanucleus.jdo.query.AnnotationProcessorUtils.TypeCategory;

import javax.jdo.query.BooleanExpression;
import javax.jdo.query.ByteExpression;
import javax.jdo.query.CharacterExpression;
import javax.jdo.query.CollectionExpression;
import javax.jdo.query.DateExpression;
import javax.jdo.query.DateTimeExpression;
import javax.jdo.query.ListExpression;
import javax.jdo.query.LocalDateExpression;
import javax.jdo.query.LocalDateTimeExpression;
import javax.jdo.query.LocalTimeExpression;
import javax.jdo.query.MapExpression;
import javax.jdo.query.NumericExpression;
import javax.jdo.query.ObjectExpression;
import javax.jdo.query.OptionalExpression;
import javax.jdo.query.PersistableExpression;
import javax.jdo.query.StringExpression;
import javax.jdo.query.TimeExpression;
import javax.jdo.JDOQLTypedQuery;

/**
 * Annotation processor for JDO to generate "dummy" classes for all persistable classes for use with the 
 * JDOQLTypedQuery API. Any class ({MyClass}) that has a JDO "class" annotation will have a stub class (Q{MyClass}) generated.
 * <ul>
 * <li>For each managed class X in package p, a metamodel class QX in package p is created.</li>
 * <li>The name of the metamodel class is derived from the name of the managed class by prepending "Q" 
 * to the name of the managed class.</li>
 * </ul>
 * 
 * <p>
 * This processor can generate classes in two modes.
 * <ul>
 * <li>Property access - so users type in "field1()", "field1().field2()" etc. 
 * Specify the compiler argument "queryMode" as "PROPERTY" to get this</li>
 * <li>Field access - so users type in "field1", "field1.field2". This is the default.</li>
 * </ul>
 */
@SupportedAnnotationTypes({"javax.jdo.annotations.PersistenceCapable"})
public class JDOQueryProcessor extends AbstractProcessor
{
    // use "javac -AqueryMode=FIELD" to use fields
    public final static String OPTION_MODE = "queryMode";

    private final static int MODE_FIELD = 1;
    private final static int MODE_PROPERTY = 2;

    private final static String CODE_INDENT = "    ";

    public int queryMode = MODE_FIELD;
    public int fieldDepth = 5;

    @Override
    public synchronized void init(ProcessingEnvironment pe)
    {
        super.init(pe);

        pe.getMessager().printMessage(Kind.NOTE, "DataNucleus JDO AnnotationProcessor for generating JDOQLTypedQuery Q classes");// TODO Where does this go?

        // Get the query mode
        String queryMode = pe.getOptions().get(OPTION_MODE);
        if (queryMode != null && queryMode.equalsIgnoreCase("FIELD"))
        {
            this.queryMode = MODE_FIELD;
        }

        // TODO Parse persistence.xml and extract names of classes that are persistable
//        pe.getElementUtils().getTypeElement(fullyQualifiedClassName);
    }

    /* (non-Javadoc)
     * @see javax.annotation.processing.AbstractProcessor#process(java.util.Set, javax.annotation.processing.RoundEnvironment)
     */
    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv)
    {
        if (roundEnv.processingOver())
        {
            return false;
        }

        Set<? extends Element> elements = roundEnv.getRootElements();
        for (Element e : elements)
        {
            if (e instanceof TypeElement)
            {
                processClass((TypeElement)e);
            }
        }
        return false;
    }

    @Override
    public SourceVersion getSupportedSourceVersion() 
    {
        return SourceVersion.latest();
    }

    /**
     * Handler for processing a JDO annotated class to create the criteria class stub.
     * @param el The class element
     */
    protected void processClass(TypeElement el)
    {
        if (el == null || !isPersistableType(el))
        {
            return;
        }

        Elements elementUtils = processingEnv.getElementUtils();

        // TODO Support specification of the location for writing the class source files
        // TODO Set references to other classes to be the class name and put the package in the imports
        String classNameFull = elementUtils.getBinaryName(el).toString();
        String pkgName = classNameFull.substring(0, classNameFull.lastIndexOf('.'));
        String classNameSimple = classNameFull.substring(classNameFull.lastIndexOf('.') + 1);
        String qclassNameSimple = getQueryClassNameForClassName(classNameSimple);
        String qclassNameFull = pkgName + "." + qclassNameSimple;
        System.out.println("DataNucleus : JDOQLTypedQuery Q class generation : " + classNameFull + " -> " + qclassNameFull);

        Map<String, TypeMirror> genericLookups = null;
        List<? extends TypeParameterElement> elTypeParams = el.getTypeParameters();
        for (TypeParameterElement elTypeParam : elTypeParams)
        {
            List<? extends TypeMirror> elTypeBounds = elTypeParam.getBounds();
            if (elTypeBounds != null && !elTypeBounds.isEmpty())
            {
                genericLookups = new HashMap<String, TypeMirror>();
                genericLookups.put(elTypeParam.toString(), elTypeBounds.get(0));
            }
        }

        try
        {
            JavaFileObject javaFile = processingEnv.getFiler().createSourceFile(qclassNameFull);
            Writer w = javaFile.openWriter();
            try
            {
                // Package declaration and imports
                w.append("package " + pkgName + ";\n");
                w.append("\n");
                w.append("import javax.annotation.Generated;\n");
                w.append("import javax.jdo.query.*;\n");
                w.append("import org.datanucleus.api.jdo.query.*;\n");
                List<? extends Element> encElems = el.getEnclosedElements();
                if (encElems != null)
                {
                    for (Element encE : encElems)
                    {
                        if (encE instanceof TypeElement)
                        {
                            TypeElement encEl = (TypeElement)encE;
                            if (isPersistableType(encEl))
                            {
                                String innerclassNameFull = elementUtils.getBinaryName(encEl).toString();
                                String innerclassNameSimple = innerclassNameFull.substring(innerclassNameFull.lastIndexOf('.') + 1);
                                String innerclassNameSimpleShort = innerclassNameSimple.substring(innerclassNameSimple.indexOf("$")+1);
                                w.append("import " + classNameFull + "." + innerclassNameSimpleShort + ";\n");
                            }
                        }
                    }
                }
                w.append("\n");

                // Class declaration
                w.append("@Generated(value=\"" + this.getClass().getName() + "\")\n");
                w.append("public class " + qclassNameSimple);
                TypeElement superEl = getPersistentSupertype(el);
                if (superEl != null)
                {
                    // "public class QASub extends QA"
                    String superClassName = elementUtils.getBinaryName(superEl).toString();
                    w.append(" extends ").append(superClassName.substring(0, superClassName.lastIndexOf('.')+1));
                    w.append(getQueryClassNameForClassName(superClassName.substring(superClassName.lastIndexOf('.')+1)));
                }
                else
                {
                    // "public class QA extends PersistableExpressionImpl<A> implements PersistableExpression<A>"
                    w.append(" extends ").append("PersistableExpressionImpl").append("<" + classNameSimple + ">");
                    w.append(" implements ").append(PersistableExpression.class.getSimpleName() + "<" + classNameSimple + ">");
                }
                w.append("\n");
                w.append("{\n");

                String indent = "    ";

                // Add static accessor for the candidate of this type
                addStaticMethodAccessors(w, indent, qclassNameSimple, classNameSimple);
                w.append("\n");

                // Add fields for persistable members
                List<? extends Element> members = getPersistentMembers(el);
                if (members != null)
                {
                    Iterator<? extends Element> iter = members.iterator();
                    while (iter.hasNext())
                    {
                        Element member = iter.next();
                        if (member.getKind() == ElementKind.FIELD ||
                            (member.getKind() == ElementKind.METHOD && AnnotationProcessorUtils.isJavaBeanGetter((ExecutableElement) member)))
                        {
                            TypeMirror type = AnnotationProcessorUtils.getDeclaredType(member);
                            if (type instanceof TypeVariable && genericLookups != null && genericLookups.containsKey(type.toString()))
                            {
                                type = genericLookups.get(type.toString());
                            }
                            String memberName = AnnotationProcessorUtils.getMemberName(member);
                            String intfName = getExpressionInterfaceNameForType(type);

                            if (intfName.startsWith(classNameFull + "."))
                            {
                                // TODO If intfName is an inner class of this class then omit this class name
                                intfName = intfName.substring(classNameFull.length()+1);
                            }
                            if (queryMode == MODE_FIELD)
                            {
                                w.append(indent).append("public final ").append(intfName);
                                w.append(" ").append(memberName).append(";\n");
                            }
                            else
                            {
                                w.append(indent).append("private ").append(intfName);
                                w.append(" ").append(memberName).append(";\n");
                            }
                        }
                    }
                }

                // ========== Constructor(PersistableExpression parent, String name, int depth) ==========
                w.append("\n");
                addConstructorWithPersistableExpression(w, indent, qclassNameSimple, superEl, members, classNameFull, genericLookups);

                // ========== Constructor(Class type, String name, ExpressionType exprType) ==========
                w.append("\n");
                addConstructorWithType(w, indent, qclassNameSimple, members, classNameFull, genericLookups);

                // Property accessors
                if (queryMode == MODE_PROPERTY && members != null)
                {
                    Iterator<? extends Element> iter = members.iterator();
                    while (iter.hasNext())
                    {
                        Element member = iter.next();
                        if (member.getKind() == ElementKind.FIELD ||
                            (member.getKind() == ElementKind.METHOD && AnnotationProcessorUtils.isJavaBeanGetter((ExecutableElement) member)))
                        {
                            w.append("\n");
                            addPropertyAccessorMethod(w, indent, member, classNameFull, genericLookups);
                        }
                    }
                }

                if (encElems != null)
                {
                    for (Element encE : encElems)
                    {
                        if (encE instanceof TypeElement)
                        {
                            String indentInner = "        ";
                            TypeElement encEl = (TypeElement)encE;
                            if (isPersistableType(encEl))
                            {
                                // Static inner class that is persistable, so needing own Qclass inlined here
                                w.append("\n");
                                System.out.println("Persistable (static) inner class " + elementUtils.getBinaryName(encEl).toString() + " really should be in own file. " +
                                    "Trying to generate Q class inlined!");

                                // TODO Support static inner persistable classes
                                String innerclassNameFull = elementUtils.getBinaryName(encEl).toString();
                                String innerclassNameSimple = innerclassNameFull.substring(innerclassNameFull.lastIndexOf('.') + 1);
                                String innerclassNameSimpleShort = innerclassNameSimple.substring(innerclassNameSimple.indexOf("$")+1);
                                String qinnerclassNameSimpleShort = getQueryClassNameForClassName(innerclassNameSimpleShort);
                                String qinnerclassNameFull = pkgName + "." + qclassNameSimple + "$" + qinnerclassNameSimpleShort;
                                System.out.println("DataNucleus : JDOQLTypedQuery Q class generation : " + innerclassNameFull + " -> " + qinnerclassNameFull);

                                // Class declaration
                                w.append(indent).append("public static class " + qinnerclassNameSimpleShort);
                                TypeElement innerSuperEl = getPersistentSupertype(encEl);
                                if (innerSuperEl != null)
                                {
                                    // "public class QASub extends QA"
                                    String superClassName = elementUtils.getBinaryName(innerSuperEl).toString();
                                    w.append(" extends ").append(superClassName.substring(0, superClassName.lastIndexOf('.')+1));
                                    w.append(getQueryClassNameForClassName(superClassName.substring(superClassName.lastIndexOf('.')+1)));
                                }
                                else
                                {
                                    // "public class QA extends PersistableExpressionImpl<A> implements PersistableExpression<A>"
                                    w.append(" extends ").append("PersistableExpressionImpl").append("<" + innerclassNameSimpleShort + ">");
                                    w.append(" implements ").append(PersistableExpression.class.getSimpleName() + "<" + innerclassNameSimpleShort + ">");
                                }
                                w.append("\n");
                                w.append(indent).append("{\n");

                                // Add static accessor for the candidate of this type
                                addStaticMethodAccessors(w, indentInner, qinnerclassNameSimpleShort, innerclassNameSimpleShort);
                                w.append("\n");

                                // Add fields for persistable members
                                List<? extends Element> innerMembers = getPersistentMembers(encEl);
                                if (members != null)
                                {
                                    Iterator<? extends Element> iter = innerMembers.iterator();
                                    while (iter.hasNext())
                                    {
                                        Element member = iter.next();
                                        if (member.getKind() == ElementKind.FIELD ||
                                            (member.getKind() == ElementKind.METHOD && AnnotationProcessorUtils.isJavaBeanGetter((ExecutableElement) member)))
                                        {
                                            TypeMirror type = AnnotationProcessorUtils.getDeclaredType(member);
                                            if (type instanceof TypeVariable && genericLookups != null && genericLookups.containsKey(type.toString()))
                                            {
                                                type = genericLookups.get(type.toString());
                                            }
                                            String memberName = AnnotationProcessorUtils.getMemberName(member);
                                            String intfName = getExpressionInterfaceNameForType(type);

                                            if (queryMode == MODE_FIELD)
                                            {
                                                w.append(indentInner).append("public final ").append(intfName).append(" ").append(memberName).append(";\n");
                                            }
                                            else
                                            {
                                                w.append(indentInner).append("private ").append(intfName).append(" ").append(memberName).append(";\n");
                                            }
                                        }
                                    }
                                }

                                // ========== Constructor(PersistableExpression parent, String name, int depth) ==========
                                w.append("\n");
                                addConstructorWithPersistableExpression(w, indentInner, qinnerclassNameSimpleShort, innerSuperEl, innerMembers, classNameFull, genericLookups);

                                // ========== Constructor(Class type, String name, ExpressionType exprType) ==========
                                w.append("\n");
                                addConstructorWithType(w, indentInner, qinnerclassNameSimpleShort, innerMembers, classNameFull, genericLookups);

                                // Property accessors
                                if (queryMode == MODE_PROPERTY && members != null)
                                {
                                    Iterator<? extends Element> iter = members.iterator();
                                    while (iter.hasNext())
                                    {
                                        Element member = iter.next();
                                        if (member.getKind() == ElementKind.FIELD ||
                                            (member.getKind() == ElementKind.METHOD && AnnotationProcessorUtils.isJavaBeanGetter((ExecutableElement) member)))
                                        {
                                            w.append("\n");
                                            addPropertyAccessorMethod(w, indentInner, member, classNameFull, genericLookups);
                                        }
                                    }
                                }

                                w.append(indent).append("}\n");
                            }
                        }
                    }
                }

                w.append("}\n");
                w.flush();
            }
            finally
            {
                w.close();
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Method to add the code for static method accessors needed by this QClass.
     * @param w The writer
     * @param indent Indent to apply to the code
     * @param qclassNameSimple Simple name of the QClass that this is constructing
     * @param classNameSimple Simple name of this persistable class
     * @throws IOException Thrown if an error occurs on writing this code
     */
    protected void addStaticMethodAccessors(Writer w, String indent, String qclassNameSimple, String classNameSimple)
    throws IOException
    {
        // Add static accessor for the candidate of this type
        w.append(indent).append("public static final ").append(qclassNameSimple).append(" jdoCandidate").append(" = candidate(\"this\");\n");
        w.append("\n");

        // Add static method to generate candidate of this type with a particular name
        w.append(indent).append("public static " + qclassNameSimple + " candidate(String name)\n");
        w.append(indent).append("{\n");
        w.append(indent).append(CODE_INDENT).append("return new ").append(qclassNameSimple).append("(null, name, " + fieldDepth + ");\n");
        w.append(indent).append("}\n");
        w.append("\n");

        // Add static method to generate candidate of this type for default name ("this")
        w.append(indent).append("public static " + qclassNameSimple + " candidate()\n");
        w.append(indent).append("{\n");
        w.append(indent).append(CODE_INDENT).append("return jdoCandidate;\n");
        w.append(indent).append("}\n");
        w.append("\n");

        // Add static method to generate parameter of this type
        w.append(indent).append("public static " + qclassNameSimple + " parameter(String name)\n");
        w.append(indent).append("{\n");
        w.append(indent).append(CODE_INDENT).append("return new ").append(qclassNameSimple).append("(" + classNameSimple + ".class, name, ExpressionType.PARAMETER);\n");
        w.append(indent).append("}\n");
        w.append("\n");

        // Add static method to generate variable of this type
        w.append(indent).append("public static " + qclassNameSimple + " variable(String name)\n");
        w.append(indent).append("{\n");
        w.append(indent).append(CODE_INDENT).append("return new ").append(qclassNameSimple).append("(" + classNameSimple + ".class, name, ExpressionType.VARIABLE);\n");
        w.append(indent).append("}\n");
    }

    /**
     * Method to add the code for a constructor taking in (PersistableExpression parent, String name, int depth).
     * @param w The writer
     * @param indent Indent to apply to the code
     * @param qclassNameSimple Simple name of the QClass that this is constructing
     * @param superEl Any super element
     * @param members Members for this QClass that need initialising
     * @param classNameFull Fully qualified class name
     * @param genericLookups Lookup for TypeVariables
     * @throws IOException Thrown if an error occurs on writing this code
     */
    protected void addConstructorWithPersistableExpression(Writer w, String indent, String qclassNameSimple, TypeElement superEl, List<? extends Element> members, String classNameFull,
            Map<String, TypeMirror> genericLookups)
    throws IOException
    {
        w.append(indent).append("public " + qclassNameSimple).append("(").append(PersistableExpression.class.getSimpleName() + " parent, String name, int depth)\n");
        w.append(indent).append("{\n");
        if (superEl != null)
        {
            w.append(indent).append(CODE_INDENT).append("super(parent, name, depth);\n");
        }
        else
        {
            w.append(indent).append(CODE_INDENT).append("super(parent, name);\n");
        }
        if (queryMode == MODE_FIELD && members != null)
        {
            // Initialise all fields
            Iterator<? extends Element> iter = members.iterator();
            while (iter.hasNext())
            {
                Element member = iter.next();
                if (member.getKind() == ElementKind.FIELD ||
                    (member.getKind() == ElementKind.METHOD && AnnotationProcessorUtils.isJavaBeanGetter((ExecutableElement) member)))
                {
                    TypeMirror type = AnnotationProcessorUtils.getDeclaredType(member);
                    if (type instanceof TypeVariable && genericLookups != null && genericLookups.containsKey(type.toString()))
                    {
                        type = genericLookups.get(type.toString());
                    }
                    String memberName = AnnotationProcessorUtils.getMemberName(member);
                    String implClassName = getExpressionImplClassNameForType(type);
                    if (implClassName.startsWith(classNameFull + "."))
                    {
                        // TODO If intfName is an inner class of this class then omit this class name
                        implClassName = implClassName.substring(classNameFull.length()+1);
                    }
                    if (isPersistableType(type))
                    {
                        // if (depth > 0)
                        // {
                        //     this.{field} = new {ImplType}(this, memberName, depth-1);
                        // }
                        // else
                        // {
                        //     this.{field} = null;
                        // }
                        w.append(indent).append(CODE_INDENT).append("if (depth > 0)\n");
                        w.append(indent).append(CODE_INDENT).append("{\n");
                        w.append(indent).append(CODE_INDENT).append(CODE_INDENT).append("this.").append(memberName).append(" = new ").append(implClassName)
                            .append("(this, \"" + memberName + "\", depth-1);\n");
                        w.append(indent).append(CODE_INDENT).append("}\n");
                        w.append(indent).append(CODE_INDENT).append("else\n");
                        w.append(indent).append(CODE_INDENT).append("{\n");
                        w.append(indent).append(CODE_INDENT).append(CODE_INDENT).append("this.").append(memberName).append(" = null;\n");
                        w.append(indent).append(CODE_INDENT).append("}\n");
                    }
                    else
                    {
                        // this.{field} = new {ImplType}(this, memberName);
                        w.append(indent).append(CODE_INDENT).append("this.").append(memberName);
                        w.append(" = new ").append(implClassName).append("(this, \"" + memberName + "\");\n");
                    }
                }
            }
        }
        w.append(indent).append("}\n");
    }

    /**
     * Method to add the code for a constructor taking in (Class type, String name, ExpressionType exprType).
     * @param w The writer
     * @param indent Indent to apply to the code
     * @param qclassNameSimple Simple name of the QClass that this is constructing
     * @param members Members for this QClass that need initialising
     * @param classNameFull Fully qualified class name
     * @param genericLookups Lookup for TypeVariables
     * @throws IOException Thrown if an error occurs on writing this code
     */
    protected void addConstructorWithType(Writer w, String indent, String qclassNameSimple, List<? extends Element> members, String classNameFull, Map<String, TypeMirror> genericLookups)
    throws IOException
    {
        w.append(indent).append("public " + qclassNameSimple).append("(").append(Class.class.getSimpleName() + " type, String name, ExpressionType exprType)\n");
        w.append(indent).append("{\n");
        w.append(indent).append(CODE_INDENT).append("super(type, name, exprType);\n");
        if (queryMode == MODE_FIELD && members != null)
        {
            // Initialise all fields
            Iterator<? extends Element> iter = members.iterator();
            while (iter.hasNext())
            {
                Element member = iter.next();
                if (member.getKind() == ElementKind.FIELD ||
                    (member.getKind() == ElementKind.METHOD && AnnotationProcessorUtils.isJavaBeanGetter((ExecutableElement) member)))
                {
                    TypeMirror type = AnnotationProcessorUtils.getDeclaredType(member);
                    if (type instanceof TypeVariable && genericLookups != null && genericLookups.containsKey(type.toString()))
                    {
                        type = genericLookups.get(type.toString());
                    }
                    String memberName = AnnotationProcessorUtils.getMemberName(member);
                    String implClassName = getExpressionImplClassNameForType(type);
                    if (implClassName.startsWith(classNameFull + "."))
                    {
                        // TODO If intfName is an inner class of this class then omit this class name
                        implClassName = implClassName.substring(classNameFull.length()+1);
                    }
                    if (isPersistableType(type))
                    {
                        // this.{field} = new {ImplType}(this, memberName, fieldDepth);
                        w.append(indent).append(CODE_INDENT).append("this.").append(memberName).append(" = new ").append(implClassName)
                            .append("(this, \"" + memberName + "\", " + fieldDepth + ");\n");
                    }
                    else
                    {
                        // this.{field} = new {ImplType}(this, memberName);
                        w.append(indent).append(CODE_INDENT).append("this.").append(memberName).append(" = new ").append(implClassName)
                            .append("(this, \"" + memberName + "\");\n");
                    }
                }
            }
        }
        w.append(indent).append("}\n");
    }

    protected void addPropertyAccessorMethod(Writer w, String indent, Element member, String classNameFull, Map<String, TypeMirror> genericLookups)
    throws IOException
    {
        // public {type} {memberName}()
        // {
        //     if (memberVar == null)
        //     {
        //         this.memberVar = new {implClassName}(this, \"memberName\");
        //     }
        //     return this.memberVar;
        // }
        TypeMirror type = AnnotationProcessorUtils.getDeclaredType(member);
        if (type instanceof TypeVariable && genericLookups != null && genericLookups.containsKey(type.toString()))
        {
            type = genericLookups.get(type.toString());
        }
        String memberName = AnnotationProcessorUtils.getMemberName(member);
        String implClassName = getExpressionImplClassNameForType(type);
        if (implClassName.startsWith(classNameFull + "."))
        {
            // TODO If intfName is an inner class of this class then omit this class name
            implClassName = implClassName.substring(classNameFull.length()+1);
        }
        String intfName = getExpressionInterfaceNameForType(type);

        w.append(indent).append("public ").append(intfName).append(" ").append(memberName).append("()\n");
        w.append(indent).append("{\n");
        w.append(indent).append(CODE_INDENT).append("if (this.").append(memberName).append(" == null)\n");
        w.append(indent).append(CODE_INDENT).append("{\n");
        w.append(indent).append(CODE_INDENT).append(CODE_INDENT).append("this." + memberName).append(" = new ").append(implClassName).append("(this, \"" + memberName + "\");\n");
        w.append(indent).append(CODE_INDENT).append("}\n");
        w.append(indent).append(CODE_INDENT).append("return this.").append(memberName).append(";\n");
        w.append(indent).append("}\n");
    }

    /**
     * Convenience method to return the query expression interface name for a specified type.
     * @param type The type
     * @return The query expression interface name to use
     */
    private String getExpressionInterfaceNameForType(TypeMirror type)
    {
        if (type.getKind() == TypeKind.DECLARED && type instanceof DeclaredType && type instanceof TypeVariable)
        {
            // This was needed to detect such as a field with a Bean Validation 2.0 @NotNull, which comes through as 
            // "(@javax.validation.constraints.NotNull :: theUserType)", so this converts that to "theUserType".
            // TODO Is this the best way to trap that case ? (i.e "TypeVariable")? probably not, so find a better way
            // Note that this is also a WildcardType, ReferenceType, ArrayType
            type = ((DeclaredType)type).asElement().asType();
        }

        if (type.getKind() == TypeKind.BOOLEAN)
        {
            return BooleanExpression.class.getSimpleName();
        }
        else if (type.getKind() == TypeKind.BYTE)
        {
            return ByteExpression.class.getSimpleName();
        }
        else if (type.getKind() == TypeKind.CHAR)
        {
            return CharacterExpression.class.getSimpleName();
        }
        else if (type.getKind() == TypeKind.DOUBLE)
        {
            return NumericExpression.class.getSimpleName() + "<Double>";
        }
        else if (type.getKind() == TypeKind.FLOAT)
        {
            return NumericExpression.class.getSimpleName() + "<Float>";
        }
        else if (type.getKind() == TypeKind.INT)
        {
            return NumericExpression.class.getSimpleName() + "<Integer>";
        }
        else if (type.getKind() == TypeKind.LONG)
        {
            return NumericExpression.class.getSimpleName() + "<Long>";
        }
        else if (type.getKind() == TypeKind.SHORT)
        {
            return NumericExpression.class.getSimpleName() + "<Short>";
        }
        else if (type.toString().equals(BigInteger.class.getName()))
        {
            return NumericExpression.class.getSimpleName() + "<java.math.BigInteger>";
        }
        else if (type.toString().equals(BigDecimal.class.getName()))
        {
            return NumericExpression.class.getSimpleName() + "<java.math.BigDecimal>";
        }
        else if (type.toString().equals(String.class.getName()))
        {
            return StringExpression.class.getSimpleName();
        }
        else if (type.toString().equals(Date.class.getName()))
        {
            return DateTimeExpression.class.getSimpleName();
        }
        else if (type.toString().equals(java.sql.Date.class.getName()))
        {
            return DateExpression.class.getSimpleName();
        }
        else if (type.toString().equals(java.sql.Time.class.getName()))
        {
            return TimeExpression.class.getSimpleName();
        }
        else if (type.toString().equals(LocalDate.class.getName()))
        {
            return LocalDateExpression.class.getSimpleName();
        }
        else if (type.toString().equals(LocalTime.class.getName()))
        {
            return LocalTimeExpression.class.getSimpleName();
        }
        else if (type.toString().equals(LocalDateTime.class.getName()))
        {
            return LocalDateTimeExpression.class.getSimpleName();
        }
        else if (type.toString().equals(java.util.Optional.class.getName()))
        {
            return OptionalExpression.class.getSimpleName();
        }

        String typeName = AnnotationProcessorUtils.getDeclaredTypeName(processingEnv, type, true);
        TypeCategory cat = AnnotationProcessorUtils.getTypeCategoryForTypeMirror(typeName);
        if (cat == TypeCategory.MAP)
        {
            return MapExpression.class.getSimpleName();
        }
        else if (cat == TypeCategory.LIST)
        {
            return ListExpression.class.getSimpleName();
        }
        else if (cat == TypeCategory.COLLECTION || cat == TypeCategory.SET)
        {
            return CollectionExpression.class.getSimpleName();
        }
        else
        {
            TypeElement typeElement = (TypeElement) processingEnv.getTypeUtils().asElement(type);
            if (typeElement != null && isPersistableType(typeElement))
            {
                // Persistent field ("mydomain.Xxx" becomes "mydomain.QXxx")
                return typeName.substring(0, typeName.lastIndexOf('.')+1) + getQueryClassNameForClassName(typeName.substring(typeName.lastIndexOf('.')+1));
            }
            else
            {
                return ObjectExpression.class.getSimpleName() + "<" + type.toString() + ">";
            }
        }
    }

    /**
     * Convenience method to return the query expression implementation name for a specified type.
     * @param type The type
     * @return The query expression implementation class name to use
     */
    private String getExpressionImplClassNameForType(TypeMirror type)
    {
        if (type.getKind() == TypeKind.DECLARED)
        {
            // Declared type, so take element type. 
            // Note this works for things like Bean Validation 2.0 @NotNull which comes through as "(@javax.validation.constraints.NotNull :: theUserType)"
            type = ((DeclaredType)type).asElement().asType();
        }

        if (type.getKind() == TypeKind.BOOLEAN)
        {
            return "BooleanExpressionImpl";
        }
        else if (type.getKind() == TypeKind.BYTE)
        {
            return "ByteExpressionImpl";
        }
        else if (type.getKind() == TypeKind.CHAR)
        {
            return "CharacterExpressionImpl";
        }
        else if (type.getKind() == TypeKind.DOUBLE)
        {
            return "NumericExpressionImpl<Double>";
        }
        else if (type.getKind() == TypeKind.FLOAT)
        {
            return "NumericExpressionImpl<Float>";
        }
        else if (type.getKind() == TypeKind.INT)
        {
            return "NumericExpressionImpl<Integer>";
        }
        else if (type.getKind() == TypeKind.LONG)
        {
            return "NumericExpressionImpl<Long>";
        }
        else if (type.getKind() == TypeKind.SHORT)
        {
            return "NumericExpressionImpl<Short>";
        }
        else if (type.toString().equals(BigInteger.class.getName()))
        {
            return "NumericExpressionImpl<java.math.BigInteger>";
        }
        else if (type.toString().equals(BigDecimal.class.getName()))
        {
            return "NumericExpressionImpl<java.math.BigDecimal>";
        }
        else if (type.toString().equals(String.class.getName()))
        {
            return "StringExpressionImpl";
        }
        else if (type.toString().equals(Date.class.getName()))
        {
            return "DateTimeExpressionImpl";
        }
        else if (type.toString().equals(java.sql.Date.class.getName()))
        {
            return "DateExpressionImpl";
        }
        else if (type.toString().equals(java.sql.Time.class.getName()))
        {
            return "TimeExpressionImpl";
        }
        else if (type.toString().equals(LocalDate.class.getName()))
        {
            return "LocalDateExpressionImpl";
        }
        else if (type.toString().equals(LocalTime.class.getName()))
        {
            return "LocalTimeExpressionImpl";
        }
        else if (type.toString().equals(LocalDateTime.class.getName()))
        {
            return "LocalDateTimeExpressionImpl";
        }
        else if (type.toString().equals(java.util.Optional.class.getName()))
        {
            return "OptionalExpressionImpl";
        }

        String typeName = AnnotationProcessorUtils.getDeclaredTypeName(processingEnv, type, true);
        TypeCategory cat = AnnotationProcessorUtils.getTypeCategoryForTypeMirror(typeName);
        if (cat == TypeCategory.MAP)
        {
            return "MapExpressionImpl";
        }
        else if (cat == TypeCategory.LIST)
        {
            return "ListExpressionImpl";
        }
        else if (cat == TypeCategory.COLLECTION || cat == TypeCategory.SET)
        {
            return "CollectionExpressionImpl";
        }
        else
        {
            TypeElement typeElement = (TypeElement) processingEnv.getTypeUtils().asElement(type);
            if (typeElement != null && isPersistableType(typeElement))
            {
                // Persistent field ("mydomain.Xxx" becomes "mydomain.QXxx")
                return typeName.substring(0, typeName.lastIndexOf('.')+1) + getQueryClassNameForClassName(typeName.substring(typeName.lastIndexOf('.')+1));
            }
            else
            {
                return "ObjectExpressionImpl<" + type.toString() + ">";
            }
        }
    }

    /**
     * Convenience method to return the query expression implementation name for a specified type.
     * @param type The type
     * @return The query expression implementation class name to use
     */
    private boolean isPersistableType(TypeMirror type)
    {
        TypeElement typeElement = (TypeElement) processingEnv.getTypeUtils().asElement(type);
        if (typeElement != null && isPersistableType(typeElement))
        {
            return true;
        }
        return false;
    }

    public boolean isPersistableType(TypeElement el)
    {
        if (el == null)
        {
            return false;
        }
        if ((el.getAnnotation(PersistenceCapable.class) != null))
        {
            return true;
        }
        // TODO Also allow for types that are specified as persistable in XML
        return false;
    }

    /**
     * Method to return the persistable members for the specified class.
     * @param el The class (TypeElement)
     * @return The members that are persistable (Element)
     */
    private static List<? extends Element> getPersistentMembers(TypeElement el)
    {
        List<? extends Element> members = AnnotationProcessorUtils.getFieldMembers(el); // All fields needed
        if (members != null)
        {
            // Remove any non-persistent members
            Iterator<? extends Element> iter = members.iterator();
            while (iter.hasNext())
            {
                Element member = iter.next();
                boolean persistent = true;

                if (member.getModifiers().contains(Modifier.STATIC))
                {
                    // Don't include static member in Q class
                    persistent = false;
                }
                else
                {
                    List<? extends AnnotationMirror> annots = member.getAnnotationMirrors();
                    if (annots != null)
                    {
                        Iterator<? extends AnnotationMirror> annotIter = annots.iterator();
                        while (annotIter.hasNext())
                        {
                            AnnotationMirror annot = annotIter.next();
                            if (annot.getAnnotationType().toString().equals(NotPersistent.class.getName()))
                            {
                                // Ignore this
                                persistent = false;
                                break;
                            }
                        }
                    }
                }
                if (!persistent)
                {
                    iter.remove();
                }
            }
        }
        return members;
    }

    /**
     * Method to find the next persistent supertype above this one.
     * @param element The element
     * @return Its next parent that is persistable (or null if no persistable predecessors)
     */
    public TypeElement getPersistentSupertype(TypeElement element)
    {
        TypeMirror superType = element.getSuperclass();
        if (superType == null || "java.lang.Object".equals(element.toString()))
        {
            return null;
        }

        TypeElement superElement = (TypeElement) processingEnv.getTypeUtils().asElement(superType);
        if (superElement == null || isPersistableType(superElement))
        {
            return superElement;
        }
        return getPersistentSupertype(superElement);
    }

    /**
     * Method to return the (simple) name of the query class for a specified class name.
     * Currently just returns "Q{className}"
     * @param name Simple name of the class (without package)
     * @return Simple name of the query class
     */
    public static String getQueryClassNameForClassName(String name)
    {
        return JDOQLTypedQuery.QUERY_CLASS_PREFIX + name;
    }
}