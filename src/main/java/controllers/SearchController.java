package controllers;

import java.util.List;

import models.OpResult;
import models.ResultDocument;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import services.ElasticSearch;

@RestController
@EnableAutoConfiguration
public class SearchController {
	
	@RequestMapping("/search/{pattern}")
	public @ResponseBody List<ResultDocument> search(@PathVariable("pattern") String pattern) throws InterruptedException
	{
		return ElasticSearch.getElasticSearch().find(pattern);
	}
	
	@RequestMapping("/add/{doc}")
	public @ResponseBody OpResult add(@PathVariable("doc") String doc) throws InterruptedException
	{
		return ElasticSearch.getElasticSearch().addDocument(doc);
		
	}

	public static void main(String[] args) {
		SpringApplication.run(SearchController.class);
		ElasticSearch.BuildElasticSearch();
	}
	
}
