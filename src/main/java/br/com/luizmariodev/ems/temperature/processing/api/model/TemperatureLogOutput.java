package br.com.luizmariodev.ems.temperature.processing.api.model;

import io.hypersistence.tsid.TSID;
import lombok.Data;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.UUID;

@Data
public class TemperatureLogOutput implements Serializable {

    private UUID id;
    private TSID sensorId;
    private OffsetDateTime registeredAt;
    private Double value;

}
