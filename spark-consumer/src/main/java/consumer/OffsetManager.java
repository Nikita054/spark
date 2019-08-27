package consumer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OffsetManager {
    private String stotagePrefix;

    public OffsetManager(String stotagePrefix){
        this.stotagePrefix=stotagePrefix;
    }

    public void saveOffsetInExternalStorage(String topic, int partition, long offset){
        try {
            FileWriter fileWriter = new FileWriter(storageName(topic,partition),false);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(offset+"");
            bufferedWriter.flush();
            bufferedWriter.close();
        }
        catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    public long readOffsetFromExternalStorage(String topic, int partition){
        try {
            Stream<String> stream= Files.lines(Paths.get(storageName(topic,partition)));
            return Long.parseLong(stream.collect(Collectors.toList()).get(0))+1;
        }
        catch (Exception e){
            System.out.println(e.getMessage());
        }
        return 0;
    }
    private String storageName(String topic, int partition){
        return stotagePrefix+topic+"-"+partition+".txt";
    }
}
