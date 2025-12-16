import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;
import org.knowm.xchart.style.lines.SeriesLines;
import org.knowm.xchart.style.markers.SeriesMarkers;

import java.awt.Color;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EtlProcessor {

    // --- CONFIGURATION ---
    private static final String VERSION = "0.03 (StatsLimit)";
    private static final String LOGS_DIR = "Logs";
    private static final String ASSETS_DIR = "assets";
    private static final String DATASET_FILE = "amala_dataset.csv";
    private static final String TEMPLATE_FILE = "index.template.md";
    private static final String OUTPUT_FILE = "index.md";

    // --- SCALING PARAMETERS ---
    private static final double SCALE_MAX_JAVA_HOURS = 10.0;
    private static final double SCALE_MAX_SLEEP_HOURS = 10.0;
    private static final double SCALE_MAX_FOREST_MINUTES = 120.0;
    private static final double JAVA_PLAN_POINTS = 6.0;

    // --- DATA MODEL (POJO) ---
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DailyLog {
        public LocalDate date;
        public String type;
        @JsonProperty("java_hours")
        public double javaHours;
        public int mood;
        public int diet;
        @JsonProperty("sleep_hours")
        public double sleepHours;
        @JsonProperty("sleep_quality")
        public int sleepQuality;
        @JsonProperty("forest_minutes")
        public int forestMinutes;
        public String link;
        public String content;

        // Шкалированные значения
        public double scaleJava;
        public double scaleSleepHours;
        public double scaleSleepQuality;
        public double scaleForest;
    }

    // --- 1. EXTRACT ---
    private static List<DailyLog> parseLogs() throws IOException {
        List<DailyLog> data = new ArrayList<>();
        File logsDir = new File(LOGS_DIR);

        if (!logsDir.exists()) {
            System.out.println("⚠️ Папка Logs не найдена.");
            return data;
        }

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.registerModule(new JavaTimeModule());

        try (Stream<Path> paths = Files.walk(Paths.get(LOGS_DIR))) {
            List<File> files = paths.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".md"))
                    .filter(path -> !path.getFileName().toString().equals("index.md"))
                    .map(Path::toFile)
                    .toList();

            System.out.println("🔍 Найдено логов: " + files.size());

            for (File file : files) {
                try {
                    String fileContent = Files.readString(file.toPath());
                    String[] parts = fileContent.split("---", 3);
                    if (parts.length < 3) {
                        System.out.println("⚠️ Некорректный формат в " + file.getName());
                        continue;
                    }
                    String yamlPart = parts[1];
                    String markdownContent = parts[2];

                    DailyLog log = mapper.readValue(yamlPart, DailyLog.class);
                    log.link = LOGS_DIR + "/" + file.getName();
                    log.content = markdownContent.strip();
                    data.add(log);
                    System.out.println("✅ Спарсено из " + file.getName());
                } catch (Exception e) {
                    System.out.println("❌ Ошибка в " + file.getName() + ": " + e.getMessage());
                }
            }
        }
        return data;
    }

    // --- 2. TRANSFORM ---
    private static List<DailyLog> processData(List<DailyLog> data) throws IOException {
        if (data.isEmpty()) return data;

        data.sort(Comparator.comparing(log -> log.date));

        double cumulativeJava = 0;
        List<Object[]> csvData = new ArrayList<>();
        String[] csvHeaders = {"date", "java_hours", "mood", "diet", "sleep_hours", "sleep_quality", "forest_minutes", "cumulative_java", "link"};
        csvData.add(csvHeaders);

        for (DailyLog log : data) {
            log.scaleJava = (log.javaHours / SCALE_MAX_JAVA_HOURS) * 10.0;
            log.scaleSleepHours = (log.sleepHours / SCALE_MAX_SLEEP_HOURS) * 10.0;
            log.scaleSleepQuality = log.sleepQuality / 10.0;
            log.scaleForest = (log.forestMinutes / SCALE_MAX_FOREST_MINUTES) * 10.0;

            cumulativeJava += log.javaHours;

            csvData.add(new Object[]{
                    log.date, log.javaHours, log.mood, log.diet,
                    log.sleepHours, log.sleepQuality, log.forestMinutes,
                    cumulativeJava, log.link
            });
        }

        try (FileWriter out = new FileWriter(DATASET_FILE);
             CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT.withHeader(csvHeaders))) {
            for (int i = 1; i < csvData.size(); i++) {
                printer.printRecord(csvData.get(i));
            }
        }
        System.out.println("💾 Датасет сохранён: " + DATASET_FILE);

        return data;
    }

    // --- 3. VISUALIZE ---
    private static void generateCharts(List<DailyLog> data) throws IOException {
        Path assetsPath = Paths.get(ASSETS_DIR);
        if (!Files.exists(assetsPath)) {
            Files.createDirectories(assetsPath);
        }

        XYChart chart = new XYChartBuilder()
                .width(1200).height(700)
                .title("Unified Correlation Dashboard (0-10 Scale)")
                .xAxisTitle("Date").yAxisTitle("Unified Score (0-10)")
                .build();

        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setYAxisMin(0.0);
        chart.getStyler().setYAxisMax(11.0);
        chart.getStyler().setPlotGridLinesVisible(true);
        chart.getStyler().setDatePattern("yyyy-MM-dd");
        chart.getStyler().setMarkerSize(5);

        List<java.util.Date> dates = data.stream()
                .map(d -> java.sql.Date.valueOf(d.date))
                .collect(Collectors.toList());

        List<Double> java = data.stream().map(d -> d.scaleJava).collect(Collectors.toList());
        List<Double> sleepHours = data.stream().map(d -> d.scaleSleepHours).collect(Collectors.toList());
        List<Double> sleepQuality = data.stream().map(d -> d.scaleSleepQuality).collect(Collectors.toList());
        List<Double> mood = data.stream().map(d -> (double) d.mood).collect(Collectors.toList());
        List<Double> diet = data.stream().map(d -> (double) d.diet).collect(Collectors.toList());
        List<Double> forest = data.stream().map(d -> d.scaleForest).collect(Collectors.toList());

        XYSeries javaSeries = chart.addSeries("Java Hours", dates, java);
        javaSeries.setLineWidth(4);
        javaSeries.setMarker(SeriesMarkers.CIRCLE);
        javaSeries.setMarkerColor(Color.decode("#2ecc71"));
        javaSeries.setLineColor(Color.decode("#2ecc71"));

        chart.addSeries("Sleep Duration", dates, sleepHours).setLineColor(Color.decode("#3498db"));
        chart.addSeries("Sleep Quality", dates, sleepQuality).setLineColor(Color.decode("#9b59b6"));
        chart.addSeries("Mood", dates, mood).setLineColor(Color.decode("#e67e22"));
        chart.addSeries("Diet", dates, diet).setLineColor(Color.decode("#e74c3c"));
        chart.addSeries("Forest Walk", dates, forest).setLineColor(Color.decode("#1abc9c"));

        if (dates.size() > 1) {
            XYSeries planSeries = chart.addSeries("Daily Plan (6h Java)",
                    List.of(dates.get(0), dates.get(dates.size() - 1)),
                    List.of(JAVA_PLAN_POINTS, JAVA_PLAN_POINTS));
            planSeries.setLineColor(Color.decode("#f1c40f"));
            planSeries.setLineStyle(SeriesLines.DASH_DASH);
            planSeries.setMarker(SeriesMarkers.NONE);
            planSeries.setLineWidth(4);
        }

        String chartPath = ASSETS_DIR + "/progress_chart.svg";
        VectorGraphicsEncoder.saveVectorGraphic(chart, chartPath, VectorGraphicsEncoder.VectorGraphicsFormat.SVG);
        System.out.println("🎨 График сохранён: " + chartPath);
    }

    // --- 4. PUBLISH (MAIN DASHBOARD) ---
    private static void updateDashboard(List<DailyLog> data) throws IOException {
        if (data.isEmpty()) return;

        double totalHours = data.stream().mapToDouble(d -> d.javaHours).sum();
        int daysCount = data.size();
        String lastUpdate = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));

        StringBuilder logListMd = new StringBuilder();
        List<DailyLog> sortedForDashboard = new ArrayList<>(data);
        sortedForDashboard.sort(Comparator.comparing(log -> log.date, Comparator.reverseOrder()));

        // --- ИЗМЕНЕНИЕ 2: Берем только последние 5 записей ---
        List<DailyLog> recentLogs = sortedForDashboard.stream().limit(5).toList();

        for (DailyLog row : recentLogs) {
            String dateStr = row.date.format(DateTimeFormatter.ISO_LOCAL_DATE);
            String link = String.format("- [**%s**](%s) — Java: `%sh` | Mood: `%d` | Diet: `%d`\n",
                    dateStr, row.link, row.javaHours, row.mood, row.diet);
            logListMd.append(link);
        }

        // Добавляем ссылку на архив, если записей больше 5
        if (data.size() > 5) {
            logListMd.append("\n[→ **View Full Archive**](Logs/index.md)\n");
        } else {
            logListMd.append("\n[→ **View Archive**](Logs/index.md)\n");
        }

        String template = Files.readString(Paths.get(TEMPLATE_FILE));
        String content = template
                .replace("{{TOTAL_HOURS}}", String.format("%.1f", totalHours))
                .replace("{{DAYS_IN}}", String.valueOf(daysCount))
                .replace("{{LAST_UPDATE}}", lastUpdate)
                .replace("{{VERSION}}", VERSION)
                .replace("{{LOG_LIST}}", logListMd.toString().strip());

        Files.writeString(Paths.get(OUTPUT_FILE), content);
        System.out.println("🚀 Дашборд обновлён: " + OUTPUT_FILE);
    }

    // --- 5. GENERATE LOGS ARCHIVE PAGE ---
    private static void generateLogsPage(List<DailyLog> data) throws IOException {
        if (data.isEmpty()) return;

        List<DailyLog> sortedData = new ArrayList<>(data);
        sortedData.sort(Comparator.comparing(log -> log.date, Comparator.reverseOrder()));

        StringBuilder logsPageContent = new StringBuilder();
        logsPageContent.append("---\n");
        logsPageContent.append("layout: default\n");
        logsPageContent.append("title: Daily Logs Archive\n");
        logsPageContent.append("---\n\n");
        logsPageContent.append("# Daily Logs Archive\n\n");
        logsPageContent.append("All entries are listed here in reverse chronological order (newest to oldest).\n\n");

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        for (DailyLog log : sortedData) {
            logsPageContent.append("---\n\n");
            logsPageContent.append("## ").append(log.date.format(formatter)).append("\n\n");

            // --- ИЗМЕНЕНИЕ 1: Вывод полной статистики перед контентом ---
            String statsLine = String.format(
                    "**Type:** `%s` | **Java:** `%.1fh` | **Mood:** `%d` | **Diet:** `%d` | **Sleep:** `%.1fh` (%d%%) | **Forest:** `%dm`",
                    log.type, log.javaHours, log.mood, log.diet, log.sleepHours, log.sleepQuality, log.forestMinutes
            );
            logsPageContent.append(statsLine).append("\n\n");

            logsPageContent.append(log.content).append("\n\n");
        }

        Path logsDirPath = Paths.get(LOGS_DIR);
        if (!Files.exists(logsDirPath)) {
            Files.createDirectories(logsDirPath);
        }

        Path outputPath = Paths.get(LOGS_DIR, "index.md");
        Files.writeString(outputPath, logsPageContent.toString());
        System.out.println("📖 Страница всех логов сгенерирована: " + outputPath);
    }

    // --- MAIN ---
    public static void main(String[] args) {
        System.out.println("--- AMALA ETL PROCESSOR (JAVA VERSION) v" + VERSION + " START ---");
        try {
            List<DailyLog> rawData = parseLogs();
            if (rawData.isEmpty()) {
                System.out.println("Нет данных для обработки. Завершение.");
                return;
            }
            List<DailyLog> cleanData = processData(rawData);
            generateCharts(cleanData);
            updateDashboard(cleanData);
            generateLogsPage(cleanData);
            System.out.println("--- SUCCESS ---");
        } catch (IOException e) {
            System.err.println("Произошла критическая ошибка: " + e.getMessage());
            e.printStackTrace();
        }
    }
}