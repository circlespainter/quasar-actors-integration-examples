package co.paralleluniverse.actors.integration.examples;

import java.io.Serializable;

public final class Msg implements Serializable {
    private static final long serialVersionUID = 0L;

    @SuppressWarnings("WeakerAccess")
    public int id;

    @SuppressWarnings("unused")
    public Msg() {} // For serialization

    public Msg(int id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Msg(" + id + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Msg msg = (Msg) o;

        return id == msg.id;

    }

    @Override
    public int hashCode() {
        return id;
    }
}
