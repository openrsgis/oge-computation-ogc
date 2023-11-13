package whu.edu.cn.trajectory.base.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author xuqi
 * @date 2023/11/06
 */
public class SerializerUtils implements Serializable {
    public SerializerUtils() {
    }

    public static byte[] serializeList(List tarList, Class tarClass) throws IOException {
        return serializeList(tarList, tarClass, new JavaSerializer());
    }

    public static byte[] serializeList(List tarList, Class tarClass, Serializer serializer)
            throws IOException {
        Kryo kryo = new Kryo();
        kryo.setReferences(false);
        kryo.setRegistrationRequired(true);
        CollectionSerializer collectionSerializer = new CollectionSerializer();
        collectionSerializer.setElementClass(tarClass, serializer);
        collectionSerializer.setElementsCanBeNull(false);
        kryo.register(tarClass, serializer);
        kryo.register(LinkedList.class, collectionSerializer);
        kryo.register(ArrayList.class, collectionSerializer);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Throwable exception = null;
        boolean hasOutputProblem = false;

        byte[] res;
        try {
            hasOutputProblem = true;
            Output output = new Output(byteArrayOutputStream, 10240);
            Throwable innerException = null;
            boolean hasToByteProblem = false;

            try {
                hasToByteProblem = true;
                kryo.writeObject(output, tarList);
                output.flush();
                res = byteArrayOutputStream.toByteArray();
                byteArrayOutputStream.flush();
                hasToByteProblem = false;
            } catch (Throwable exception1) {
                innerException = exception1;
                throw exception1;
            } finally {
                if (hasToByteProblem) {
                    if (innerException != null) {
                        try {
                            output.close();
                        } catch (Throwable exception2) {
                            innerException.addSuppressed(exception2);
                        }
                    } else {
                        output.close();
                    }

                }
            }
            output.close();
            hasOutputProblem = false;
        } catch (Throwable exception3) {
            exception = exception3;
            throw exception3;
        } finally {
            if (hasOutputProblem) {
                if (exception != null) {
                    try {
                        byteArrayOutputStream.close();
                    } catch (Throwable exception4) {
                        exception.addSuppressed(exception4);
                    }
                } else {
                    byteArrayOutputStream.close();
                }
            }
        }
        byteArrayOutputStream.close();
        return res;
    }

    public static List deserializeList(byte[] tarList, Class tarClass) throws IOException {
        return deserializeList(tarList, tarClass, new JavaSerializer());
    }

    public static List deserializeList(byte[] tarList, Class tarClass, Serializer serializer)
            throws IOException {
        Kryo kryo = new Kryo();
        if (tarClass.getClassLoader() != null) {
            kryo.setClassLoader(tarClass.getClassLoader());
        }
        kryo.setReferences(false);
        kryo.setRegistrationRequired(true);
        CollectionSerializer collectionSerializer = new CollectionSerializer();
        collectionSerializer.setElementClass(tarClass, serializer);
        collectionSerializer.setElementsCanBeNull(false);
        kryo.register(tarClass, serializer);
        kryo.register(ArrayList.class, collectionSerializer);
        kryo.register(LinkedList.class, collectionSerializer);

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(tarList);
        Throwable exception = null;
        boolean hasInputProblem = false;

        LinkedList res;
        try {
            hasInputProblem = true;
            Input input = new Input(byteArrayInputStream);
            Throwable innerException = null;
            boolean hasFromByteProblem = false;

            try {
                hasFromByteProblem = true;
                res = (LinkedList) kryo.readObject(input, LinkedList.class, collectionSerializer);
                hasFromByteProblem = false;
            } catch (Throwable exception1) {
                innerException = exception1;
                throw exception1;
            } finally {
                if (hasFromByteProblem) {
                    if (innerException != null) {
                        try {
                            input.close();
                        } catch (Throwable exception2) {
                            innerException.addSuppressed(exception2);
                        }
                    } else {
                        input.close();
                    }

                }
            }

            input.close();
            hasInputProblem = false;
        } catch (Throwable exception3) {
            exception = exception3;
            throw exception3;
        } finally {
            if (hasInputProblem) {
                if (exception != null) {
                    try {
                        byteArrayInputStream.close();
                    } catch (Throwable exception4) {
                        exception.addSuppressed(exception4);
                    }
                } else {
                    byteArrayInputStream.close();
                }

            }
        }
        byteArrayInputStream.close();
        return res;
    }

    public static byte[] serializeObject(Serializable tarObject) throws IOException {
        return serializeObject(tarObject, new JavaSerializer());
    }

    public static Serializable deserializeObject(byte[] tarObjectByteList, Class tarClass) {
        if (tarObjectByteList == null) {
            return null;
        }
        return deserializeObject(tarObjectByteList, tarClass, new JavaSerializer());
    }

    public static byte[] serializeObject(Serializable tarObject, Serializer serializer)
            throws IOException {
        Kryo kryo = new Kryo();
        kryo.setReferences(false);
        kryo.register(tarObject.getClass(), serializer);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Throwable exception = null;
        boolean hasOutputProblem = false;

        byte[] res;
        try {
            hasOutputProblem = true;
            Output output = new Output(byteArrayOutputStream, 10240);
            kryo.writeClassAndObject(output, tarObject);
            output.flush();
            output.close();
            res = byteArrayOutputStream.toByteArray();
            hasOutputProblem = false;
        } catch (Throwable exception1) {
            exception = exception1;
            throw exception1;
        } finally {
            if (hasOutputProblem) {
                if (exception != null) {
                    try {
                        byteArrayOutputStream.close();
                    } catch (Throwable exception2) {
                        exception.addSuppressed(exception2);
                    }
                } else {
                    byteArrayOutputStream.close();
                }

            }
        }

        byteArrayOutputStream.close();
        return res;
    }

    public static Serializable deserializeObject(byte[] tarObjectByteList, Class tarClass,
                                                 Serializer serializer) {
        Kryo kryo = new Kryo();
        if (tarClass.getClassLoader() != null) {
            kryo.setClassLoader(tarClass.getClassLoader());
        }
        kryo.setReferences(false);
        kryo.register(tarClass, serializer);
        kryo.setRegistrationRequired(true);

        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(tarObjectByteList);
            Throwable exception = null;
            boolean hasInputProblem = false;

            Serializable res;
            try {
                hasInputProblem = true;
                Input input = new Input(byteArrayInputStream);
                Throwable innerException = null;
                boolean hasFromByteProblem = false;

                try {
                    hasFromByteProblem = true;
                    res = (Serializable) kryo.readClassAndObject(input);
                    hasFromByteProblem = false;
                } catch (Throwable exception1) {
                    innerException = exception1;
                    throw exception1;
                } finally {
                    if (hasFromByteProblem) {
                        if (innerException != null) {
                            try {
                                input.close();
                            } catch (Throwable exception2) {
                                innerException.addSuppressed(exception2);
                            }
                        } else {
                            input.close();
                        }

                    }
                }

                input.close();
                hasInputProblem = false;
            } catch (Throwable exception3) {
                exception = exception3;
                throw exception3;
            } finally {
                if (hasInputProblem) {
                    if (exception != null) {
                        try {
                            byteArrayInputStream.close();
                        } catch (Throwable exception4) {
                            exception.addSuppressed(exception4);
                        }
                    } else {
                        byteArrayInputStream.close();
                    }

                }
            }

            byteArrayInputStream.close();
            return res;
        } catch (IOException exception5) {
            exception5.printStackTrace();
            return null;
        }
    }
}
