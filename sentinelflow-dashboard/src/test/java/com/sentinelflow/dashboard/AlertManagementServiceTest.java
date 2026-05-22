package com.sentinelflow.dashboard;

import com.sentinelflow.dashboard.entity.Alert;
import com.sentinelflow.dashboard.repository.AlertRepository;
import com.sentinelflow.dashboard.service.AlertManagementService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AlertManagementServiceTest {

    @Mock
    private AlertRepository alertRepository;

    private AlertManagementService service;

    @BeforeEach
    void setUp() {
        service = new AlertManagementService(alertRepository);
    }

    @Test
    void shouldCreateAlert() {
        when(alertRepository.save(any())).thenAnswer(i -> i.getArgument(0));

        var alert = service.createAlert("High CPU", "CRITICAL", "order-service",
                "CPU at 95%", "Check pods", "anom-1");

        assertThat(alert.getId()).isNotBlank();
        assertThat(alert.getStatus()).isEqualTo("OPEN");
        assertThat(alert.getSeverity()).isEqualTo("CRITICAL");
        verify(alertRepository).save(any());
    }

    @Test
    void shouldTransitionOpenToAck() {
        var existing = new Alert("a1", "Test", "WARNING", "OPEN", "svc",
                null, null, null, null, null);
        when(alertRepository.findById("a1")).thenReturn(Optional.of(existing));
        when(alertRepository.save(any())).thenAnswer(i -> i.getArgument(0));

        var result = service.updateStatus("a1", "ACK");

        assertThat(result).isPresent();
        assertThat(result.get().getStatus()).isEqualTo("ACK");
    }

    @Test
    void shouldTransitionOpenToResolved() {
        var existing = new Alert("a2", "Test", "WARNING", "OPEN", "svc",
                null, null, null, null, null);
        when(alertRepository.findById("a2")).thenReturn(Optional.of(existing));
        when(alertRepository.save(any())).thenAnswer(i -> i.getArgument(0));

        var result = service.updateStatus("a2", "RESOLVED");

        assertThat(result).isPresent();
        assertThat(result.get().getStatus()).isEqualTo("RESOLVED");
        assertThat(result.get().getResolvedAt()).isNotNull();
    }

    @Test
    void shouldRejectInvalidTransition() {
        var existing = new Alert("a3", "Test", "WARNING", "RESOLVED", "svc",
                null, null, null, null, null);
        when(alertRepository.findById("a3")).thenReturn(Optional.of(existing));

        assertThatThrownBy(() -> service.updateStatus("a3", "OPEN"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
