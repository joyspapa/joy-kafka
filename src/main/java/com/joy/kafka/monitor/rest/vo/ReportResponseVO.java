package com.joy.kafka.monitor.rest.vo;

import java.util.ArrayList;
import java.util.List;

import com.joy.kafka.monitor.config.ViewType;
import com.joy.kafka.monitor.report.ReportVO;

public class ReportResponseVO extends ResponseVO {

	private List<ReportVO> results = new ArrayList<>();
	private boolean isReport = true;

	public ReportResponseVO(ViewType viewType, List<ReportVO> resultList) {
		super(viewType);
		this.results = resultList;
	}
	
	public List<ReportVO> getResults() {
		return results;
	}

	public void setResults(List<ReportVO> results) {
		this.results = results;
	}

	public boolean isReport() {
		return isReport;
	}

	public void setReport(boolean isReport) {
		this.isReport = isReport;
	}
}
