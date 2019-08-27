package dto;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class Forecasting implements Serializable {
    private String NOC;
    private String description;
    private String industry;
    private String variable;
    private String geografickArea;
    private Integer year2018;
    private Integer year2019;
    private Integer year2020;
    private Integer year2021;
    private Integer year2022;
    private Integer year2023;
    private Integer year2024;
    private Integer year2025;
    private Integer year2026;
    private Integer year2027;
    private Integer year2028;
    private Double fistFiveYearPercentage;
    private Double secondFiveYearPercentage;
    private Double decadePercentage;
}
