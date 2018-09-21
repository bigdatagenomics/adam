/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//
// Copied from shade-maven-plugin with modifications

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.bdgenomics.adam.shade;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipException;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.shade.Shader;
import org.apache.maven.plugins.shade.ShadeRequest;
import org.apache.maven.plugins.shade.filter.Filter;
import org.apache.maven.plugins.shade.relocation.Relocator;
import org.apache.maven.plugins.shade.resource.ManifestResourceTransformer;
import org.apache.maven.plugins.shade.resource.ResourceTransformer;
import org.codehaus.plexus.component.annotations.Component;
import org.codehaus.plexus.logging.AbstractLogEnabled;
import org.codehaus.plexus.util.IOUtil;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.commons.ClassRemapper;
import org.objectweb.asm.commons.Remapper;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * @author Jason van Zyl
 */
@Component( role = Shader.class, hint = "workaround" )
public class ParquetAvroWorkaroundShader
    extends AbstractLogEnabled
    implements Shader
{

    public void shade( ShadeRequest shadeRequest )
        throws IOException, MojoExecutionException
    {
        Set<String> resources = new HashSet<String>();

        ResourceTransformer manifestTransformer = null;
        List<ResourceTransformer> transformers =
            new ArrayList<ResourceTransformer>( shadeRequest.getResourceTransformers() );
        for ( Iterator<ResourceTransformer> it = transformers.iterator(); it.hasNext(); )
        {
            ResourceTransformer transformer = it.next();
            if ( transformer instanceof ManifestResourceTransformer )
            {
                manifestTransformer = transformer;
                it.remove();
            }
        }

        RelocatorRemapper remapper = new RelocatorRemapper( shadeRequest.getRelocators() );

        // noinspection ResultOfMethodCallIgnored
        shadeRequest.getUberJar().getParentFile().mkdirs();

        JarOutputStream out = null;
        try
        {
            out = new JarOutputStream( new BufferedOutputStream( new FileOutputStream( shadeRequest.getUberJar() ) ) );
            goThroughAllJarEntriesForManifestTransformer( shadeRequest, resources, manifestTransformer, out );

            // CHECKSTYLE_OFF: MagicNumber
            Multimap<String, File> duplicates = HashMultimap.create( 10000, 3 );
            // CHECKSTYLE_ON: MagicNumber

            shadeJars( shadeRequest, resources, transformers, remapper, out, duplicates );

            // CHECKSTYLE_OFF: MagicNumber
            Multimap<Collection<File>, String> overlapping = HashMultimap.create( 20, 15 );
            // CHECKSTYLE_ON: MagicNumber

            for ( String clazz : duplicates.keySet() )
            {
                Collection<File> jarz = duplicates.get( clazz );
                if ( jarz.size() > 1 )
                {
                    overlapping.put( jarz, clazz );
                }
            }

            // Log a summary of duplicates
            logSummaryOfDuplicates( overlapping );

            if ( overlapping.keySet().size() > 0 )
            {
                showOverlappingWarning();
            }

            for ( ResourceTransformer transformer : transformers )
            {
                if ( transformer.hasTransformedResource() )
                {
                    transformer.modifyOutputStream( out );
                }
            }

            out.close();
            out = null;
        }
        finally
        {
            IOUtil.close( out );
        }

        for ( Filter filter : shadeRequest.getFilters() )
        {
            filter.finished();
        }
    }

    private void shadeJars( ShadeRequest shadeRequest, Set<String> resources, List<ResourceTransformer> transformers,
                            RelocatorRemapper remapper, JarOutputStream jos, Multimap<String, File> duplicates )
        throws IOException, MojoExecutionException
    {
        for ( File jar : shadeRequest.getJars() )
        {

            getLogger().debug( "Processing JAR " + jar );

            List<Filter> jarFilters = getFilters( jar, shadeRequest.getFilters() );

            JarFile jarFile = newJarFile( jar );

            try
            {

                for ( Enumeration<JarEntry> j = jarFile.entries(); j.hasMoreElements(); )
                {
                    JarEntry entry = j.nextElement();

                    String name = entry.getName();
                    
                    if ( entry.isDirectory() || isFiltered( jarFilters, name ) )
                    {
                        continue;
                    }


                    if ( "META-INF/INDEX.LIST".equals( name ) )
                    {
                        // we cannot allow the jar indexes to be copied over or the
                        // jar is useless. Ideally, we could create a new one
                        // later
                        continue;
                    }
                    
                    if ( "module-info.class".equals( name ) )
                    {
                        getLogger().warn( "Discovered module-info.class. "
                            + "Shading will break its strong encapsulation." );
                        continue;
                    }

                    try
                    {
                        shadeSingleJar( shadeRequest, resources, transformers, remapper, jos, duplicates, jar,
                                        jarFile, entry, name );
                    }
                    catch ( Exception e )
                    {
                        throw new IOException( String.format( "Problem shading JAR %s entry %s: %s", jar, name, e ),
                                               e );
                    }
                }

            }
            finally
            {
                jarFile.close();
            }
        }
    }

    private void shadeSingleJar( ShadeRequest shadeRequest, Set<String> resources,
                                 List<ResourceTransformer> transformers, RelocatorRemapper remapper,
                                 JarOutputStream jos, Multimap<String, File> duplicates, File jar, JarFile jarFile,
                                 JarEntry entry, String name )
        throws IOException, MojoExecutionException
    {
        InputStream in = null;
        try
        {
            in = jarFile.getInputStream( entry );
            String mappedName = remapper.map( name );

            int idx = mappedName.lastIndexOf( '/' );
            if ( idx != -1 )
            {
                // make sure dirs are created
                String dir = mappedName.substring( 0, idx );
                if ( !resources.contains( dir ) )
                {
                    addDirectory( resources, jos, dir );
                }
            }

            if ( name.endsWith( ".class" ) )
            {
                if ( jar.toString().contains( "parquet-avro" ) && ( name.contains ( "AvroSchemaConverter" ) ) )
                {
                    getLogger().warn( "WORKAROUND:  refusing to add class " + name + " from jar " + jar );
                }
                else
                {
                    duplicates.put( name, jar );
                    addRemappedClass( remapper, jos, jar, name, in );
                }
            }
            else if ( shadeRequest.isShadeSourcesContent() && name.endsWith( ".java" ) )
            {
                // Avoid duplicates
                if ( resources.contains( mappedName ) )
                {
                    return;
                }

                addJavaSource( resources, jos, mappedName, in, shadeRequest.getRelocators() );
            }
            else
            {
                if ( !resourceTransformed( transformers, mappedName, in, shadeRequest.getRelocators() ) )
                {
                    // Avoid duplicates that aren't accounted for by the resource transformers
                    if ( resources.contains( mappedName ) )
                    {
                        return;
                    }

                    addResource( resources, jos, mappedName, entry.getTime(), in );
                }
            }

            in.close();
            in = null;
        }
        finally
        {
            IOUtil.close( in );
        }
    }

    private void goThroughAllJarEntriesForManifestTransformer( ShadeRequest shadeRequest, Set<String> resources,
                                                               ResourceTransformer manifestTransformer,
                                                               JarOutputStream jos )
        throws IOException
    {
        if ( manifestTransformer != null )
        {
            for ( File jar : shadeRequest.getJars() )
            {
                JarFile jarFile = newJarFile( jar );
                try
                {
                    for ( Enumeration<JarEntry> en = jarFile.entries(); en.hasMoreElements(); )
                    {
                        JarEntry entry = en.nextElement();
                        String resource = entry.getName();
                        if ( manifestTransformer.canTransformResource( resource ) )
                        {
                            resources.add( resource );
                            InputStream inputStream = jarFile.getInputStream( entry );
                            try
                            {
                                manifestTransformer.processResource( resource, inputStream,
                                                                     shadeRequest.getRelocators() );
                            }
                            finally
                            {
                                inputStream.close();
                            }
                            break;
                        }
                    }
                }
                finally
                {
                    jarFile.close();
                }
            }
            if ( manifestTransformer.hasTransformedResource() )
            {
                manifestTransformer.modifyOutputStream( jos );
            }
        }
    }

    private void showOverlappingWarning()
    {
        getLogger().warn( "maven-shade-plugin has detected that some class files are" );
        getLogger().warn( "present in two or more JARs. When this happens, only one" );
        getLogger().warn( "single version of the class is copied to the uber jar." );
        getLogger().warn( "Usually this is not harmful and you can skip these warnings," );
        getLogger().warn( "otherwise try to manually exclude artifacts based on" );
        getLogger().warn( "mvn dependency:tree -Ddetail=true and the above output." );
        getLogger().warn( "See http://maven.apache.org/plugins/maven-shade-plugin/" );
    }

    private void logSummaryOfDuplicates( Multimap<Collection<File>, String> overlapping )
    {
        for ( Collection<File> jarz : overlapping.keySet() )
        {
            List<String> jarzS = new LinkedList<String>();

            for ( File jjar : jarz )
            {
                jarzS.add( jjar.getName() );
            }

            List<String> classes = new LinkedList<String>();

            for ( String clazz : overlapping.get( jarz ) )
            {
                classes.add( clazz.replace( ".class", "" ).replace( "/", "." ) );
            }

            //CHECKSTYLE_OFF: LineLength
            getLogger().warn(
                Joiner.on( ", " ).join( jarzS ) + " define " + classes.size() + " overlapping classes: " );
            //CHECKSTYLE_ON: LineLength

            int max = 10;

            for ( int i = 0; i < Math.min( max, classes.size() ); i++ )
            {
                getLogger().warn( "  - " + classes.get( i ) );
            }

            if ( classes.size() > max )
            {
                getLogger().warn( "  - " + ( classes.size() - max ) + " more..." );
            }

        }
    }

    private JarFile newJarFile( File jar )
        throws IOException
    {
        try
        {
            return new JarFile( jar );
        }
        catch ( ZipException zex )
        {
            // JarFile is not very verbose and doesn't tell the user which file it was
            // so we will create a new Exception instead
            throw new ZipException( "error in opening zip file " + jar );
        }
    }

    private List<Filter> getFilters( File jar, List<Filter> filters )
    {
        List<Filter> list = new ArrayList<Filter>();

        for ( Filter filter : filters )
        {
            if ( filter.canFilter( jar ) )
            {
                list.add( filter );
            }

        }

        return list;
    }

    private void addDirectory( Set<String> resources, JarOutputStream jos, String name )
        throws IOException
    {
        if ( name.lastIndexOf( '/' ) > 0 )
        {
            String parent = name.substring( 0, name.lastIndexOf( '/' ) );
            if ( !resources.contains( parent ) )
            {
                addDirectory( resources, jos, parent );
            }
        }

        // directory entries must end in "/"
        JarEntry entry = new JarEntry( name + "/" );
        jos.putNextEntry( entry );

        resources.add( name );
    }

    private void addRemappedClass( RelocatorRemapper remapper, JarOutputStream jos, File jar, String name,
                                   InputStream is )
        throws IOException, MojoExecutionException
    {
        if ( !remapper.hasRelocators() )
        {
            try
            {
                jos.putNextEntry( new JarEntry( name ) );
                IOUtil.copy( is, jos );
            }
            catch ( ZipException e )
            {
                getLogger().debug( "We have a duplicate " + name + " in " + jar );
            }

            return;
        }

        ClassReader cr = new ClassReader( is );

        // We don't pass the ClassReader here. This forces the ClassWriter to rebuild the constant pool.
        // Copying the original constant pool should be avoided because it would keep references
        // to the original class names. This is not a problem at runtime (because these entries in the
        // constant pool are never used), but confuses some tools such as Felix' maven-bundle-plugin
        // that use the constant pool to determine the dependencies of a class.
        ClassWriter cw = new ClassWriter( 0 );

        final String pkg = name.substring( 0, name.lastIndexOf( '/' ) + 1 );
        ClassVisitor cv = new ClassRemapper( cw, remapper )
        {
            @Override
            public void visitSource( final String source, final String debug )
            {
                if ( source == null )
                {
                    super.visitSource( source, debug );
                }
                else
                {
                    final String fqSource = pkg + source;
                    final String mappedSource = remapper.map( fqSource );
                    final String filename = mappedSource.substring( mappedSource.lastIndexOf( '/' ) + 1 );
                    super.visitSource( filename, debug );
                }
            }
        };

        try
        {
            cr.accept( cv, ClassReader.EXPAND_FRAMES );
        }
        catch ( Throwable ise )
        {
            throw new MojoExecutionException( "Error in ASM processing class " + name, ise );
        }

        byte[] renamedClass = cw.toByteArray();

        // Need to take the .class off for remapping evaluation
        String mappedName = remapper.map( name.substring( 0, name.indexOf( '.' ) ) );

        try
        {
            // Now we put it back on so the class file is written out with the right extension.
            jos.putNextEntry( new JarEntry( mappedName + ".class" ) );

            IOUtil.copy( renamedClass, jos );
        }
        catch ( ZipException e )
        {
            getLogger().debug( "We have a duplicate " + mappedName + " in " + jar );
        }
    }

    private boolean isFiltered( List<Filter> filters, String name )
    {
        for ( Filter filter : filters )
        {
            if ( filter.isFiltered( name ) )
            {
                return true;
            }
        }

        return false;
    }

    private boolean resourceTransformed( List<ResourceTransformer> resourceTransformers, String name, InputStream is,
                                         List<Relocator> relocators )
        throws IOException
    {
        boolean resourceTransformed = false;

        for ( ResourceTransformer transformer : resourceTransformers )
        {
            if ( transformer.canTransformResource( name ) )
            {
                getLogger().debug( "Transforming " + name + " using " + transformer.getClass().getName() );

                transformer.processResource( name, is, relocators );

                resourceTransformed = true;

                break;
            }
        }
        return resourceTransformed;
    }

    private void addJavaSource( Set<String> resources, JarOutputStream jos, String name, InputStream is,
                                List<Relocator> relocators )
        throws IOException
    {
        jos.putNextEntry( new JarEntry( name ) );

        String sourceContent = IOUtil.toString( new InputStreamReader( is, "UTF-8" ) );

        for ( Relocator relocator : relocators )
        {
            sourceContent = relocator.applyToSourceContent( sourceContent );
        }

        final Writer writer = new OutputStreamWriter( jos, "UTF-8" );
        IOUtil.copy( sourceContent, writer );
        writer.flush();

        resources.add( name );
    }

    private void addResource( Set<String> resources, JarOutputStream jos, String name, long lastModified,
                              InputStream is )
        throws IOException
    {
        final JarEntry jarEntry = new JarEntry( name );

        jarEntry.setTime( lastModified );

        jos.putNextEntry( jarEntry );

        IOUtil.copy( is, jos );

        resources.add( name );
    }

    static class RelocatorRemapper
        extends Remapper
    {

        private final Pattern classPattern = Pattern.compile( "(\\[*)?L(.+);" );

        List<Relocator> relocators;

        RelocatorRemapper( List<Relocator> relocators )
        {
            this.relocators = relocators;
        }

        public boolean hasRelocators()
        {
            return !relocators.isEmpty();
        }

        public Object mapValue( Object object )
        {
            if ( object instanceof String )
            {
                String name = (String) object;
                String value = name;

                String prefix = "";
                String suffix = "";

                Matcher m = classPattern.matcher( name );
                if ( m.matches() )
                {
                    prefix = m.group( 1 ) + "L";
                    suffix = ";";
                    name = m.group( 2 );
                }

                for ( Relocator r : relocators )
                {
                    if ( r.canRelocateClass( name ) )
                    {
                        value = prefix + r.relocateClass( name ) + suffix;
                        break;
                    }
                    else if ( r.canRelocatePath( name ) )
                    {
                        value = prefix + r.relocatePath( name ) + suffix;
                        break;
                    }
                }

                return value;
            }

            return super.mapValue( object );
        }

        public String map( String name )
        {
            String value = name;

            String prefix = "";
            String suffix = "";

            Matcher m = classPattern.matcher( name );
            if ( m.matches() )
            {
                prefix = m.group( 1 ) + "L";
                suffix = ";";
                name = m.group( 2 );
            }

            for ( Relocator r : relocators )
            {
                if ( r.canRelocatePath( name ) )
                {
                    value = prefix + r.relocatePath( name ) + suffix;
                    break;
                }
            }

            return value;
        }

    }

}
