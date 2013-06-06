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

package edu.berkeley.cs.amplab.adam.modules;

import edu.berkeley.cs.amplab.adam.AdamMain;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

public abstract class AdamModule extends Configured implements Tool {

    public abstract String getModuleName();

    public abstract String getModuleDescription();

    public abstract int moduleRun() throws Exception;

    @Override
    public int run(String[] args) throws Exception {
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(AdamMain.HEADER + "\n");
            System.err.println(e.getMessage());
            System.err.println("\nList of valid options:\n");
            parser.printUsage(System.err);
            System.err.println("\n" + AdamMain.FOOTER);
            return -1;
        }
        return moduleRun();
    }
}
