/*
 * Copyright (c) 2013. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.amplab.adam;

import edu.berkeley.amplab.adam.modules.AdamModule;
import edu.berkeley.amplab.adam.modules.BAMImport;
import edu.berkeley.amplab.adam.modules.FASTAImport;
import edu.berkeley.amplab.adam.modules.PrintFile;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;

public class AdamMain {
    public static final String HEADER = "ADAM: Datastore Alignment Map";
    public static final String FOOTER = "For more information, see http://github.com/massie/adam";

    // We could load modules dynamically in the future...
    public static List<AdamModule> MODULES = new LinkedList<AdamModule>();

    static {
        MODULES.add(new PrintFile());
        MODULES.add(new BAMImport());
        MODULES.add(new FASTAImport());
    }

    private static void printHelpAndExit() {
        StringBuilder sb = new StringBuilder().append(HEADER).append("\n\n");
        sb.append("The commandline syntax is:\n");
        sb.append("  $ bin/hadoop jar adam-X.Y.jar [generic options] moduleName [module options]\n");
        sb.append("The generic hadoop options are listed below.\n");
        sb.append("The module options are provided by the module, e.g.:\n");
        sb.append("  $ bin/hadoop jar adam-X.Y.jar moduleName\n");
        sb.append("Please choose one of the following modules:\n\n")
                .append(String.format("  %32s\t%s\n", "Module Name", "Description"))
                .append(String.format("  %32s\t%s\n", "-----------", "-----------"));
        for (AdamModule module : MODULES) {
            sb.append(
                    String.format("  %32s\t%s", module.getModuleName(), module.getModuleDescription()))
                    .append("\n");
        }
        sb.append("\n\n");
        // Print the support Generic Options
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        ToolRunner.printGenericCommandUsage(ps);
        sb.append(baos.toString());
        ps.close();
        sb.append(FOOTER).append("\n");
        System.err.println(sb.toString());
        System.exit(1);
    }

    private static AdamModule findApp(String moduleName) {
        for (AdamModule module : MODULES) {
            if (module.getModuleName().equals(moduleName)) {
                return module;
            }
        }
        return null;
    }

    /**
     * The args comming in should look like...
     * <p/>
     * [generic options] moduleName [module options]
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printHelpAndExit();
        }
        // Args after removing all generic args
        String[] nonGenericArgs = new GenericOptionsParser(args).getRemainingArgs();
        if (nonGenericArgs.length == 0) {
            // We need at least an module name
            printHelpAndExit();
        }
        // The module name should be the first arg after the generic options
        String moduleName = nonGenericArgs[0];
        AdamModule module = findApp(moduleName);
        if (module == null) {
            System.err.println(String.format("Unknown module name='%s'", moduleName));
            printHelpAndExit();
        } else {
            String[] argsWithoutProgramName = new String[args.length - 1];
            boolean nameFound = false;
            int i = 0;
            for (String arg : args) {
                if (!nameFound && moduleName.equals(arg)) {
                    nameFound = true;
                    continue;
                }
                argsWithoutProgramName[i++] = arg;
            }
            // We run the tool after removing the program name from the arguments
            // NOTE: the actual method that is run here is AdamModule.run()
            int res = ToolRunner.run(module, argsWithoutProgramName);
            System.exit(res);
        }
    }

}
