package com.big.data.engineering3.pipelines;

import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.json.JSONObject;

import javax.annotation.Nullable;

public class StudentAssessmentWriteCsvPerWindow extends PTransform<PCollection<String>, PDone> {
    private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.hourMinute();
    private String filenamePrefix;
    @Nullable
    private Integer numShards;

    public StudentAssessmentWriteCsvPerWindow(String filenamePrefix, Integer numShards) {
        this.filenamePrefix = filenamePrefix;
        this.numShards = numShards;
    }

    public PDone expand(PCollection<String> input) {
        ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(this.filenamePrefix);
        TextIO.Write write = TextIO.write()
                .to(new PerWindowFiles(resource))
                .withTempDirectory(resource.getCurrentDirectory())
                .withWindowedWrites()
                .withHeader("id_assessment,id_student,date_submitted,is_banked,score");
        if (this.numShards != null) {
            write = write.withNumShards(this.numShards);
        }

        PCollection<String> csvStrings = input.apply("Convert to CSV", ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                JSONObject json = new JSONObject(c.element());
                String idAssessment = json.getString("idAssessment");
                String idStudent = json.getString("isStudent");
                String dateSubmitted = json.getString("dateSubmitted");
                String isBanked = json.getString("isBanked");
                String score = json.getString("score");
                String csvString = String.format("%s,%s,%s,%s,%s", idAssessment, idStudent, dateSubmitted, isBanked, score);
                c.output(csvString);
            }
        }));

        return csvStrings.apply(write);
    }

    public static class PerWindowFiles extends FileBasedSink.FilenamePolicy {
        private final ResourceId baseFilename;

        public PerWindowFiles(ResourceId baseFilename) {
            this.baseFilename = baseFilename;
        }

        public String filenamePrefixForWindow(IntervalWindow window) {
            String prefix = this.baseFilename.isDirectory() ? "" : (String) MoreObjects.firstNonNull(this.baseFilename.getFilename(), "");
            return String.format("%s-%s-%s", prefix, FORMATTER.print(window.start()), FORMATTER.print(window.end()));
        }

        public ResourceId windowedFilename(int shardNumber, int numShards, BoundedWindow window, PaneInfo paneInfo, FileBasedSink.OutputFileHints outputFileHints) {
            IntervalWindow intervalWindow = (IntervalWindow) window;
//            String filename = String.format("%s-%s-of-%s%s", this.filenamePrefixForWindow(intervalWindow), shardNumber, numShards, outputFileHints.getSuggestedFilenameSuffix());
            String filename = String.format("%s-%s-of-%s%s", this.filenamePrefixForWindow(intervalWindow), shardNumber, numShards, ".csv");
            return this.baseFilename.getCurrentDirectory().resolve(filename, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
        }

        public ResourceId unwindowedFilename(int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
            throw new UnsupportedOperationException("Unsupported.");
        }
    }
}