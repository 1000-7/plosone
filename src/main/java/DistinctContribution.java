import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.sql.SQLException;
import java.util.concurrent.*;

public class DistinctContribution extends PlosOne {
    public static final String QUERYSQL = "Select * from distinctcontribution ";
    public static final String INSERTSQL = "update distinctcontribution set class = ? where id = ?";

    public static final Stemmer stemmer = new Stemmer();
    public static String[] AnalyzedData = new String[]{"Analyzed", "analysis", "interpreted", "statistical", "discussion", "figure", "analyzed", "analyses", "validation",
            "examination", "evaluate", "test", "reevaluation", "measurement", "illustrating", "generated", "critical appraisal", "visualization", "analyses", "sample",
            "analytical", "analyzed", "computations", "data extraction", "extract", "illustrations", "validated", "process", "calculations", "data abstraction",
            "interpretation", "identification", "synthesis", "synthesized", "proved", "Derived", "diagnosed", "judged", "mining", "came up with", "simulations", "drawing", "grading", "score", "implications", "quantitive", "table", "tables", "case study", "graph", "report", "description", "map", "mapping", "compare", "compared", "fig", "scoring", "digital", "digitalization", "verified", "estimate", "appraised", "intepreted", "drew", "comprison", "comprisons"};
    public static String[] DesignExperment = new String[]{"designed", "design", "conceive", "supervisor", "supervised", "meet", "inclusion", "coordination", "oversaw", "direct", "theory",
            "conduct", "responsible", "method", "model", "advice", "guide", "algorithm", "algorithmic", "lead", "led", "headed", "pursuit", "idea", "pioneer", "critical reading",
            "accountable", "entire study", "advised", "organizing", "conceptualized", "study design", "suggestion", "theory", "project conception",
            "experiments", "methdology", "constructed", "planning", "strategy", "mathematics", "framework", "concept", "overall", "chair", "logistic help", "propose", "support", "expertise", "new", "adapted", "adapt", "significant", "significantly", "formulated", "formulate", "charge", "resolved", "resolve", "inspiration", "devised", "original", "logistics", "logistic", "pointed out", "proofing", "resolution", "oversight", "specialist", " intellectual", "hypothesis", "policy", "anthropologist", "promoted", "solved", "pediatrician", "co-ordinated", "determination", "refine", "refined", "defined", "define", "proof", "discover", "discovered", "corresponding", "proofed", "start", "starting", "PI", "consult", "consultant", "team", "group", "surgeons", "arbiter", "Trouble-shooting", "understanding", "above", "physicians", "radiologist", "consolidated", "chief", "epidemiologist", "consultation", "problem", "question", "governance", "frame", "framework", "pathology"
    };
    public static String[] PerformExperment = new String[]{"Performed", "Perfomed", "performed", "perform", "technical support", "carried", "executed", "code", "assay", "patient's care",
            "troubleshooting of experiments", "software", "implemented", "enrolled", "development", "anaesthetist", "trapped mamals", "treated patients",
            "screen", "identified", "recognize", "project administration", "assembly", "monitoring", "animal experiment", "clinical assistance",
            "programming", "surgery", "cloning", "purified", "made", "web surfaces", "surfaces", "Diagnosis", "Operated", "web interface", "built", "Optimized",
            "isolated", "ran", "Detected", "Did", "trial", "Engineering", "trained", "recorded", "deployment", "application", "replication", "sequencing", "Production of recombinant proteins", "Conscious sedation procedure of bronchoscopy", "procedure", "procedures", "technical", "technics", "setup", "train", "training", "reord", "recording", " sequences", "sequence", "cultivated", "cultivate", "manufacturing", "crystal", "crystallization", "use", "using", "keep", "keeping", "trained", "user", "stimulate", "stimulator", "segmentation", "sort", "sorting", "sorted", "fabrication", "fabricate", "create", "created", "set up", "purification", "lab", "Estimated the parameters", "equipment", "install", "installation", "build", "hardware", "crystallized", "fly"};
    public static String[] WritePaper = new String[]{"written", "manuscript's", "write", "wrote", "manuscript", "edit", "revise", "review", "literature", "draft", "background",
            "final", "composing", "translations", "researched historic documents", "grammatical", "proofread", "revised", "revision", "Modified", "polish",
            "completed", "graphics", "imaging", "photograph", "author", "amend", "plot", "plotting", "plotted", "article", "checked", "english", "redaction", "corrected",
            "introduced", "pictures", "culture", "artwork", "version", "versions", "language", "abstractor", "abstracting", "historical study", "submission", "content", "publication", "details", "artwork", "text", "bibliographic research", "result", "conclusion", "scripting", "script", "grammar", "spelling", "paper", "co - wrote", "read", "reading"};
    public static String[] CollectData = new String[]{"collect", "material", "reagent", "manage", "breed", "obtain", "permission",
            "provided", "Provided", "acquisition", "contribute data", "prepare", "accessed", "data set", "data entry", "gather", "format", "database", "sampling", "annotation",
            "survey", "site visit", "recruitment", "recruited", "participant", "essential antibodies", "quality control", "cell line", "grant", "availability",
            "commented", "data curation", "investigation", "resources", "animal", "aquired", "georeferenced", "supplied", "collation", "raw data", "data complication",
            "parse", "provision", "seed", "selection", "guarantor", "data", "took care", "bred", "obtained", "feed", "treating", "Assessed", "input", "assisted", "Clinical",
            "permit", "rats", "Patients", "mouse", "saved", "stored", "access", "antibodies", "Retrieved", "fieldwork", "germplasm", "field", "trip", "rearing", "excavated", "registeration", "manually", "information", "labeling", "label", "follow-up", "assignment", "institutional", "employed", "employ", "employment", "searched", "search", "field work", "barcoding", "MRI", "fMRI", "scanning", "scan", "contact", "contected", "Taxonomic", "phenotyping", "compile", "compling", "mice", "care", "birds", "pollen", "lake", "classification", "taxomony", "community"
    };
    public static String[] Other = new String[]{"fund", "purchase", "financial", "technical assistance", "genetic support", "Lab organisation", "decision", "other",
            "Facilitated", "payment", "mentor", "administrative", "initiated", "Hosted", "initiation", "groundwork", "feedback", "instrument", "stuff", "ERC", "GIS", "pay", "paid", "cost", "budgets", "secured", "fee", "tool"};


    public DistinctContribution(String begin, String end) {
        super();
        conn = initDB();
        stmt = createstmt(conn);
        pstmt = createptmt(conn, INSERTSQL);
        rs = query(stmt, QUERYSQL, begin, end);
    }

    @Override
    public void run() {
        try {
            int num = 0;
            while (rs.next()) {
                String contribution = rs.getString("contribution").toLowerCase();
                num++;
                if (num % 2 == 0) {
                    pstmt.executeBatch();
                    conn.commit();
                    pstmt.clearBatch();
                    System.out.println(Thread.currentThread().getName() + "  " + contribution);
                }
                int id = rs.getInt("id");

                pstmt.setInt(2, id);
                if (findcategory(contribution, PerformExperment)) {
                    pstmt.setInt(1, 3);
                    pstmt.addBatch();
                    continue;
                } else if (findcategory(contribution, Other)) {
                    pstmt.setInt(1, 6);
                    pstmt.addBatch();
                    continue;
                } else if (findcategory(contribution, DesignExperment)) {
                    pstmt.setInt(1, 2);
                    pstmt.addBatch();
                    continue;
                } else if (findcategory(contribution, AnalyzedData)) {
                    pstmt.setInt(1, 1);
                    pstmt.addBatch();
                    continue;
                } else if (findcategory(contribution, CollectData)) {
                    pstmt.setInt(1, 5);
                    pstmt.addBatch();
                    continue;
                } else if (findcategory(contribution, WritePaper)) {
                    pstmt.setInt(1, 4);
                    pstmt.addBatch();
                    continue;
                } else {
                    pstmt.setInt(1, -1);
                    pstmt.addBatch();
                    continue;
                }


            }
            pstmt.executeBatch();
            conn.commit();
            pstmt.clearBatch();
            System.out.println(Thread.currentThread().getName() + " aaaaaaaaaaaaa ");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private boolean findcategory(String contribution, String[] category) {
        String s = stem(contribution.toLowerCase()).toLowerCase();
        for (String c : category) {
            String one = stem(c.toLowerCase()).toLowerCase();
            if (s.contains(one)) {
                return true;
            }
        }
        return false;
    }

    private String stem(String contribution) {
        String[] strings = contribution.split("[ /]");
        StringBuffer sb = new StringBuffer();
        for (String a : strings) {
            char[] chars = a.toCharArray();
            stemmer.add(chars, chars.length);
            stemmer.stem();
            sb.append(stemmer.toString() + " ");
        }
        return String.valueOf(sb).trim();
    }

    public static void main(String[] args) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("plosone-%d").build();
        ExecutorService pool = new ThreadPoolExecutor(10, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1024), threadFactory);
        for (int i = 0; i < 1; i++) {
            PlosOne po = new DistinctContribution(String.valueOf(i * 20000), String.valueOf((i + 1) * 20000));
            pool.execute(po);
        }
        pool.shutdown();
    }
}
