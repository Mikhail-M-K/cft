package com.company;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class Cft {

    static String regex = "\\d+";
    static Pattern patternInt = Pattern.compile(regex);
    static Pattern flagPattern = Pattern.compile("-(a|d|s|i)");
    static Logger logger =Logger.getAnonymousLogger();
    static ExecutorService pool = Executors.newCachedThreadPool();

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        while (!checkArgs(args)) {
            args = inputArgs();
        }

        String outputFilename = null;
        List<String> filesToProcess = new LinkedList<>();
        boolean isAscending = true;
        DataType dataType = null;

        for (String arg : args) {
            if (flagPattern.matcher(arg).matches()) {
                switch (arg) {
                    case "-a" :
                        break;
                    case "-d" :
                        isAscending = false;
                        break;
                    case "-i":
                        dataType = DataType.INTEGER;
                        break;
                    case "-s":
                        dataType = DataType.STRING;
                        break;
                }
            } else {
                if (outputFilename == null) {
                    outputFilename = arg;
                } else {
                    filesToProcess.add(arg);
                }
            }
        }

        Set<String> tempFiles = new HashSet<>();
        while (filesToProcess.size() > 1) {
            List<String> newFilesToProcess = new LinkedList<>();
            List<Future<String>> tasks = new ArrayList<>();
            for (int i = 0; i < filesToProcess.size(); i = i + 2) {
                if (i < filesToProcess.size() - 1 ) {
                    int finalI = i;
                    boolean finalIsAscending = isAscending;
                    DataType finalDataType = dataType;
                    List<String> finalFilesToProcess = filesToProcess;
                    tasks.add(pool.submit(() ->
                            mergeFilesToTemp(finalFilesToProcess.get(finalI), finalFilesToProcess.get(finalI + 1), finalIsAscending, finalDataType)));
                } else {
                    newFilesToProcess.add(filesToProcess.get(i));
                }
            }
            for (Future<String> task : tasks) {
                newFilesToProcess.add(task.get());
            }
            tempFiles.addAll(newFilesToProcess.subList(1, newFilesToProcess.size()));

            /*if (filesToProcess.size() % 2 != 0) { //исключение нечетного файла из списка на удаление
                tempFiles.remove(filesToProcess.get(filesToProcess.size() - 1));
            }*/

            logger.info("РАЗМЕР НОВОГО ПУЛА ФАЙЛОВ ДЛЯ ОБРАБОТКИ: " + newFilesToProcess.size());
            filesToProcess = newFilesToProcess;
        }
        pool.shutdown();
        finishMerge(filesToProcess.get(0), outputFilename);
        deleteTempFiles(tempFiles);
    }

    private static String mergeFilesToTemp(String file1, String file2, boolean isAscending, DataType dataType) {
        String tempFileName = "temp_" + UUID.randomUUID() + ".txt";
        logger.info("МЁРЖИМ: " + file1 + " и " + file2 + " в " + tempFileName);
        try(
                FileReader fileReader1 = new FileReader(file1);
                FileReader fileReader2 = new FileReader(file2);
                FileWriter writer = new FileWriter(tempFileName);
                BufferedReader br1 = new BufferedReader(fileReader1);
                BufferedReader br2 = new BufferedReader(fileReader2)
        ) {


            String line1 = searchTypeLine(br1,dataType);
            String line2 = searchTypeLine(br2,dataType);

            while (line1 != null || line2 != null) {
                if (line1 == null) {
                    writer.write(line2 + "\n");
                    line2 = searchTypeLine(br2,dataType);
                } else if (line2 == null) {
                    writer.write(line1 + "\n");
                    line1 = searchTypeLine(br1,dataType);
                } else {
                    int compare = compareDueToType(line1, line2, dataType);
                    if (isAscending) {
                        if (compare <= 0) {
                            writer.write(line1 + "\n");
                            line1 = searchTypeLine(br1,dataType);
                        } else {
                            writer.write(line2 + "\n");
                            line2 = searchTypeLine(br2,dataType);
                                                    }
                    } else {
                        if (compare <= 0) {
                            writer.write(line2 + "\n");
                            line2 = searchTypeLine(br2,dataType);
                        } else {
                            writer.write(line1 + "\n");
                            line1 = searchTypeLine(br1,dataType);
                        }
                    }
                }
            }
        } catch (FileNotFoundException e) {
            System.err.println("Файл не найден: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Возникла ошибка при чтении файла: " + e.getMessage());
        }
        return tempFileName;
    }

    private static String searchTypeLine (BufferedReader br, DataType dataType) throws IOException {
        String line = br.readLine();
        while (line != null && dataType == DataType.INTEGER && !patternInt.matcher(line).matches()) {
            line = br.readLine();
        }
        while (line != null && dataType == DataType.STRING && (line.contains(" ")||line.contains("\t") || line.isEmpty())) {
            line = br.readLine();
        }
        return line;
    }


    private static int compareDueToType(String line1, String line2, DataType dataType) {
        if (dataType == DataType.STRING) {
            return line1.compareTo(line2);
        } else {
            return Integer.compare(Integer.parseInt(line1), Integer.parseInt(line2));
        }
    }

    private static void finishMerge(String s, String outputFilename) {
        File result = new File(s);
        File target = new File(outputFilename);
        boolean success = result.renameTo(target);
        logger.info("СЛИВАЕМ " + s + " в " + outputFilename + " , удачно:  " + success);
    }

    private static boolean checkArgs (String[] args) {
         return checkNumArgs(args) && checkTypeSort(args) && checkFile(args);
    }

    private static boolean checkFile (String[] args) {
        int index = -1;
        boolean isCorr = true;
        for (String arg : args) {//получение индекса после которого должны начаться названия файлов
            index++;
            if (arg.equals("-s") || arg.equals("-i")) {
                break;
            }
        }

        if (new File(args[index + 1]).isFile()) {//проверка на существование файла с именем исходящего
            System.out.println("Файл с именем " + args[index + 1] + " существует");
            isCorr = false;
        }

        for (int i = index + 2; i < args.length; i++) {//проверка файлов на существование
            if (!new File(args[i]).isFile()) {
                System.out.println("Проблемы с файлом: " + args[i]);
                isCorr = false;
            }

        }
        return isCorr;
    }

    private static boolean checkTypeSort (String[] args) {//проверка что в первые идут
        if (args[0].equals("-a") || args[0].equals("-d")) {
            if (args[1].equals("-s") || args[1].equals("-i")) {
                return true;
            }
        }
        return args[0].equals("-s") || args[0].equals("-i");
    }
    private static boolean checkNumArgs(String[] args) {
        if(!(args.length > 2)) {
            System.out.println("Количество аргументов меньше 3");
        }
        return args.length > 2; //проверка чтобы арггументов было как минимум 3
    }

    private static String[] inputArgs() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Аргументы не валидны. Введите новые.\n" +
                "Требования для аргументов (аргумменты вводить через пробел):\n" +
                "1. режим сортировки (-a или -d), необязательный, по умолчанию сортируем по возрастанию;\n" +
                "2. тип данных (-s или -i), обязательный;\n" +
                "3. имя выходного файла, обязательное;\n" +
                "4. остальные параметры – имена входных файлов, не менее одного.\n" +
                "Пример: -d -s out.txt in1.txt in2.txt in3.txt");
        return scanner.nextLine().split(" ");
    }


    private static void deleteTempFiles(Set<String> tempFiles) {
        tempFiles.forEach(f -> {
            boolean delete = new File(f).delete();
        });
    }

    enum DataType {
        STRING,
        INTEGER
    }
}
