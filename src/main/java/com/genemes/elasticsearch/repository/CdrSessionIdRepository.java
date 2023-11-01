package com.genemes.elasticsearch.repository;

import com.genemes.elasticsearch.model.CdrSessionId;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface CdrSessionIdRepository extends ElasticsearchRepository<CdrSessionId, String> {

}
