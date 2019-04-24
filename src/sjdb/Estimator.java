package sjdb;

import org.w3c.dom.Attr;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class Estimator implements PlanVisitor {


	public Estimator() {
		// empty constructor
	}

	/* 
	 * Create output relation on Scan operator
	 *
	 * Example implementation of visit method for Scan operators.
	 */
	public void visit(Scan op) {
		Relation input = op.getRelation();
		Relation output = new Relation(input.getTupleCount());
		
		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			output.addAttribute(new Attribute(iter.next()));
		}
		
		op.setOutput(output);
	}

	public void visit(Project op) {
		Relation input = op.getInput().getOutput();
		Relation output = new Relation(input.getTupleCount());

		Attribute now;
		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			now = iter.next();
			if(op.getAttributes().contains(now))
				output.addAttribute(new Attribute(now));
		}

		op.setOutput(output);
	}
	
	public void visit(Select op) {
	}
	
	public void visit(Product op) {
		Relation left = op.getLeft().getOutput();
		Relation right = op.getRight().getOutput();
		Relation output = new Relation(left.getTupleCount()*right.getTupleCount());

		Iterator<Attribute> iter_l = left.getAttributes().iterator();
		while (iter_l.hasNext()) {
			output.addAttribute(new Attribute(iter_l.next()));
		}

		Iterator<Attribute> iter_r = right.getAttributes().iterator();
		while (iter_r.hasNext()) {
			output.addAttribute(new Attribute(iter_r.next()));
		}

		op.setOutput(output);
	}
	
	public void visit(Join op) {
	}
}
