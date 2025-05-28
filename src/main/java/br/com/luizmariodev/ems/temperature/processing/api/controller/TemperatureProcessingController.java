package br.com.luizmariodev.ems.temperature.processing.api.controller;


import br.com.luizmariodev.ems.temperature.processing.api.model.TemperatureLogOutput;
import br.com.luizmariodev.ems.temperature.processing.common.IdGenerator;
import io.hypersistence.tsid.TSID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.OffsetDateTime;

@RequestMapping("/v1/sensors/{sensorId}/temperature/data")
@RestController
@Slf4j
public class TemperatureProcessingController {

    private final RabbitTemplate rabbitTemplate;

    @Value("${exchange.temperature.received}")
    private String exchangeReceived;

    public TemperatureProcessingController(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }


    @PostMapping(consumes = MediaType.TEXT_PLAIN_VALUE)
    public void data(@PathVariable TSID sensorId, @RequestBody String text) {
        if (text == null || text.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }

        Double value;

        try {
            value = Double.parseDouble(text);
        } catch (NumberFormatException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }

        var logOutput = new TemperatureLogOutput();
        logOutput.setId(IdGenerator.generateTimeBasedUUID());
        logOutput.setSensorId(sensorId);
        logOutput.setRegisteredAt(OffsetDateTime.now());
        logOutput.setValue(value);

        log.info(logOutput.toString());

        rabbitTemplate.convertAndSend(exchangeReceived, "", logOutput);
    }
}
