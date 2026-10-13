package com.phonotech.streaming.enrichment;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Enrichment process function that adds product catalog data to raw events.
 * Routes records that fail processing to the dead-letter side output.
 */
public class EnrichmentProcessFunction extends ProcessFunction<String, String> {

    private final OutputTag<String> dlqTag;
    private transient Map<String, ProductInfo> catalog;

    public EnrichmentProcessFunction(OutputTag<String> dlqTag) {
        this.dlqTag = dlqTag;
    }

    @Override
    public void open(Configuration parameters) {
        // Pre-generate a static product catalog (500 products)
        catalog = new HashMap<>();
        String[] categories = {"Electronics", "Clothing", "Home", "Sports", "Books", "Toys", "Food", "Beauty"};
        String[] brands = {"BrandA", "BrandB", "BrandC", "BrandD", "BrandE", "BrandF", "BrandG", "BrandH"};
        Random rng = new Random(42);

        for (int i = 0; i < 500; i++) {
            String prodId = String.format("prod-%04d", i);
            catalog.put(prodId, new ProductInfo(
                    categories[rng.nextInt(categories.length)],
                    brands[rng.nextInt(brands.length)],
                    10.0 + rng.nextDouble() * 490.0
            ));
        }
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) {
        try {
            // Simple JSON enrichment: find product_id, append catalog fields
            // Events arrive as JSON strings written by the load generator
            String productId = extractField(value, "product_id");

            if (productId != null && catalog.containsKey(productId)) {
                ProductInfo info = catalog.get(productId);
                // Append enrichment fields before the closing brace
                String enriched = value.substring(0, value.lastIndexOf('}'))
                        + ",\"category\":\"" + info.category + "\""
                        + ",\"brand\":\"" + info.brand + "\""
                        + ",\"base_price\":" + String.format("%.2f", info.basePrice)
                        + ",\"enriched\":true"
                        + "}";
                out.collect(enriched);
            } else {
                // No product_id or not in catalog, pass through with flag
                String passthrough = value.substring(0, value.lastIndexOf('}'))
                        + ",\"enriched\":false}";
                out.collect(passthrough);
            }
        } catch (Exception e) {
            // Deserialization or processing failure -> dead letter
            String dlqMessage = "{\"original\":" + escapeJson(value)
                    + ",\"error\":\"" + escapeJson(e.getMessage())
                    + "\",\"timestamp\":" + System.currentTimeMillis() + "}";
            ctx.output(dlqTag, dlqMessage);
        }
    }

    private String extractField(String json, String field) {
        String pattern = "\"" + field + "\":\"";
        int start = json.indexOf(pattern);
        if (start < 0) return null;
        start += pattern.length();
        int end = json.indexOf('"', start);
        if (end < 0) return null;
        return json.substring(start, end);
    }

    private String escapeJson(String s) {
        if (s == null) return "null";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static class ProductInfo implements java.io.Serializable {
        final String category;
        final String brand;
        final double basePrice;

        ProductInfo(String category, String brand, double basePrice) {
            this.category = category;
            this.brand = brand;
            this.basePrice = basePrice;
        }
    }
}
