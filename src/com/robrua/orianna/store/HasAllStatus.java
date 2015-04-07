package com.robrua.orianna.store;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "hasall")
public class HasAllStatus {
    @Id
    private Class<?> clazz;
    private boolean hasAll;

    /*
     * (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object obj) {
        if(this == obj) {
            return true;
        }
        if(obj == null) {
            return false;
        }
        if(!(obj instanceof HasAllStatus)) {
            return false;
        }
        final HasAllStatus other = (HasAllStatus)obj;
        if(clazz == null) {
            if(other.clazz != null) {
                return false;
            }
        }
        else if(!clazz.equals(other.clazz)) {
            return false;
        }
        if(hasAll != other.hasAll) {
            return false;
        }
        return true;
    }

    /**
     * @return the clazz
     */
    public Class<?> getClazz() {
        return clazz;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (clazz == null ? 0 : clazz.hashCode());
        result = prime * result + (hasAll ? 1231 : 1237);
        return result;
    }

    /**
     * @return the hasAll
     */
    public boolean isHasAll() {
        return hasAll;
    }

    /**
     * @param clazz
     *            the clazz to set
     */
    public void setClazz(final Class<?> clazz) {
        this.clazz = clazz;
    }

    /**
     * @param hasAll
     *            the hasAll to set
     */
    public void setHasAll(final boolean hasAll) {
        this.hasAll = hasAll;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return clazz.getName() + ": " + hasAll;
    }
}
