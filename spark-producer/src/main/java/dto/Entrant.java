package dto;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class Entrant implements Serializable {
    private String fio;
    private Integer rus;
    private Integer bio;
    private Integer xim;
    private Integer individualArchivement;


}
