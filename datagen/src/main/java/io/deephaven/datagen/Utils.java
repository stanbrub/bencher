package io.deephaven.datagen;

import java.io.File;

public class Utils {
    public static File locateFile(final File dir, final String generatorFilename) {
        if (generatorFilename.startsWith(File.separator)) {
            final File file = new File(generatorFilename);
            if (!file.exists()) {
                throw new IllegalArgumentException("Couldn't find file " + file.getAbsolutePath());
            }
            return file;
        }
        String generatorAbsolutePath = dir.getAbsolutePath() + File.separator + generatorFilename;
        File file = new File(generatorAbsolutePath);
        if (!file.exists()) {
            generatorAbsolutePath = dir.getParent() + File.separator + generatorFilename;
            file = new File(generatorAbsolutePath);
            if (!file.exists()) {
                throw new IllegalArgumentException(
                        "Couldn't find file \"" + generatorFilename + "\" in \"" + dir.getPath() + "\" or its parent.");
            }
        }
        return file;
    }
}
