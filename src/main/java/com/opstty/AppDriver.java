package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");

            programDriver.addClass("listDistrict", ListDistrict.class,
                    "A map/reduce job that lists all districts containing trees");

            programDriver.addClass("listExistingSpecies", ListExistingSpecies.class,
                    "A map/reduce job that lists the different species trees");

            programDriver.addClass("countTreesByKinds", CountTreesByKinds.class,
                    "A map/reduce job that calculates the number of trees of each kinds");

            programDriver.addClass("maxHeightPerKind", MaxHeightPerKind.class,
                    "A map/reduce job that calculates the height of the tallest tree of each kind");

            programDriver.addClass("sortByHeight", SortByHeight.class,
                    "A map/reduce job that sort the trees height from smallest to largest");

            programDriver.addClass("oldestTreesDistrict", OldestTreeDistrict.class,
                    "A map/reduce job that calculates the district containing the oldest tree");

            programDriver.addClass("mostTreesDistrict", MostTreesDistrict.class,
                    "A map/reduce job that displays the district that contains the most trees");



            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
