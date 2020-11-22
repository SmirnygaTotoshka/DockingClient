/**
 * 
 */
package ru.smirnygatotoshka.docking;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author SmirnygaTotoshka
 *
 */
public class Vector3 implements Writable{
	//TODO - delete
	/* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
		return x + "," + y + "," + z;
    }

	private float x;
	
	private float y;
	
	private float z;
	/**
	 * 
	 */
	public Vector3(float x, float y, float z) {
		super();
		this.x = x;
		this.y = y;
		this.z = z;
	}

	public Vector3(Vector3 vector) {
		super();
		this.x = vector.getX();
		this.y = vector.getY();
		this.z = vector.getZ();
	}
	
	public Vector3 getVector(){
		return this;		
	}
	
	public void setVector(Vector3 vector){
		setVector(vector.getX(),vector.getY(),vector.getZ());
	}
	
	public void setVector(float x,float y,float z){
		this.x = x;
		this.y = y;
		this.z = z;
	}
	 
	public Vector3 add(float x,float y,float z) {
		return new Vector3(this.x + x,this.y + y,this.z + z);
	}
	
	public Vector3 add(Vector3 vector) {
		return add(vector.getX(),vector.getY(),vector.getZ());
	}
	
	public Vector3 minus(float x,float y,float z) {
		return new Vector3(this.x - x,this.y - y,this.z - z);
	}
	
	public Vector3 minus(Vector3 vector) {
		return minus(vector.getX(),vector.getY(),vector.getZ());
	}
	
	public Vector3 divideIntoNumber(float number) throws ArithmeticException{
	    if (number == 0) throw new ArithmeticException("Деление вектора на 0!");
		return new Vector3(this.x / number,this.y / number,this.z / number);
	}
	
	public Vector3 multipyByNumber(float number){
		return new Vector3(this.x * number,this.y * number,this.z * number);
	}
	
	public float getX() {
		return x;
	}
	public float getY() {
		return y;
	}
	public float getZ() {
		return z;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException
	{
	    x = arg0.readFloat();
	    y = arg0.readFloat();
	    z = arg0.readFloat();
	    
	}

	@Override
	public void write(DataOutput arg0) throws IOException
	{
	    arg0.writeFloat(x);
	    arg0.writeFloat(y);
	    arg0.writeFloat(z);
	}
}
