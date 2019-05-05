package sjdb;

import java.util.*;

public class Optimiser {
    Catalogue catal;
    Estimator est;
    List<Operator> scans;
    PriorityQueue<Operator> selects_Value;
    PriorityQueue<Operator> selects_Att;
    PriorityQueue<Predicate> selects_Join;
    PriorityQueue<Join> joins;
    HashMap<Operator, Operator> select_scan_coupled;
    HashMap<Attribute,Scan> all_scans_att;
    List<Scan> used_scans ;
    List<Operator> op_for_product;
    List<Product> products;
    public Optimiser(Catalogue c){
        this.catal= c;
        est = new Estimator();
        scans = new ArrayList<>();
        selects_Value = new PriorityQueue<Operator>();
        selects_Att = new PriorityQueue<Operator>();
        selects_Join = new PriorityQueue<>((Predicate o1, Predicate o2) -> {
            return Math.min(o1.getLeftAttribute().getValueCount(),o2.getRightAttribute().getValueCount()) - Math.min(o2.getLeftAttribute().getValueCount(),o2.getRightAttribute().getValueCount());
        });
        joins = new PriorityQueue<>((Join j1, Join j2) ->{
            return Math.min(j1.getPredicate().getLeftAttribute().getValueCount(),j2.getPredicate().getRightAttribute().getValueCount()) - Math.min(j2.getPredicate().getLeftAttribute().getValueCount(),j1.getPredicate().getRightAttribute().getValueCount());
        });
        select_scan_coupled = new HashMap<>();
        all_scans_att = new HashMap<>();
        used_scans=new ArrayList<>();
        op_for_product=new ArrayList<>();
        products= new ArrayList<>();
    }

    public Operator optimise(Operator pl)
    {
        schelet(pl);

        ScanCoupledWithSelect();

        JoinCreation();

        ProductCreation();
        System.out.println("hey");

        return  null;
    }
    public void schelet(Operator pl)
    {

        if(pl instanceof Select )
        {
            Select temp = (Select) pl;
            System.out.println(temp.getPredicate());
            schelet(temp.getInput());
            Selecting(temp);

        }
        if(pl instanceof Project )
        {
            Project temp = (Project) pl;
            System.out.println(temp.getAttributes());
            schelet(temp.getInput());

        }
        if(pl instanceof Scan )
        {
            Scan temp = (Scan) pl;
            Scanning(temp);
            System.out.println(temp.toString());

        }
        if(pl instanceof Product )
        {
            Product temp = (Product) pl;
            System.out.println("it's a product");
            schelet(temp.getLeft());
            schelet(temp.getRight());

        }

    }

    public void Scanning(Scan sc)
    {
        NamedRelation current = (NamedRelation) sc.getRelation();
        NamedRelation new_sc = new NamedRelation(current.toString(),current.getTupleCount());

        Iterator<Attribute> new_att= current.getAttributes().iterator();
        Attribute now;
        while (new_att.hasNext())
        {
            now= new_att.next();
            new_sc.addAttribute(new Attribute(now));
        }

        Scan scan_op= new Scan(new_sc);
        Iterator<Attribute> att= ((Scan) scan_op).getRelation().getAttributes().iterator();
        Attribute new_at;
        while(att.hasNext()){
            new_at = att.next();
            all_scans_att.put(new_at,scan_op);
        }
        scan_op.accept(est);
        scans.add(scan_op);
    }

    public void Selecting(Select sl)
    {
        if(sl.getPredicate().getRightValue() == null)
        {
            Attribute left_old = sl.getPredicate().getLeftAttribute();
            Attribute right_old = sl.getPredicate().getRightAttribute();

            Attribute left_new = new Attribute(left_old.getName(), left_old.getValueCount());
            Attribute right_new = new Attribute(right_old.getName(), right_old.getValueCount());
            Predicate pred = new Predicate(left_new, right_new);

            selects_Join.add(pred);
        }else {
            Attribute left = sl.getPredicate().getLeftAttribute();
            Attribute new_left = new Attribute(left.getName(), left.getValueCount());
            String right_val = sl.getPredicate().getRightValue();
            Predicate pred = new Predicate(new_left, right_val);

            Iterator<Operator> it = scans.iterator();
            Operator now;
            while (it.hasNext()) {
                now = it.next();
                try {
                    if (now.output.getAttributes().contains(sl.getPredicate().getLeftAttribute())) {
                        Select new_sel = new Select(now, pred);
                        new_sel.accept(est);
                        selects_Value.add(new_sel);

                    }
                } catch (ArrayIndexOutOfBoundsException e) {

                }
            }
        }
    }
    public void ScanCoupledWithSelect(){
        Iterator<Operator> it = selects_Value.iterator();
        Operator current;
        while(it.hasNext()){
            current = it.next();
            if(current instanceof Select){
                if(select_scan_coupled.get(((Select) current).getInput())==null){
                    select_scan_coupled.put(((Select) current).getInput(),current);
                }else{
                    Select new_sl = new Select(select_scan_coupled.get(((Select) current).getInput()),((Select) current).getPredicate());
                    new_sl.accept(est);

                    select_scan_coupled.put(((Select) current).getInput(), new_sl);
                }
            }
        }

        Iterator<Operator> it_scan = scans.iterator();
        while(it_scan.hasNext()){
            Operator now = it_scan.next();
            if(select_scan_coupled.get(now) != null){

            }else{
                select_scan_coupled.get(now);
            }
        }
    }

    public void JoinCreation() {
        Attribute aux;
        Attribute aux2;
        Scan new_rel = null;
        Scan new_rel2 = null;
        Operator temp = null;
        Operator temp_r = null;
        for (Predicate sc : selects_Join) {
            aux = sc.getLeftAttribute();
            aux2 = sc.getRightAttribute();
            if (all_scans_att.containsKey(aux)) {
                new_rel = all_scans_att.get(aux);
                temp = new_rel;

                while (select_scan_coupled.containsKey(temp)) {
                    temp = select_scan_coupled.get(temp);
                }
            }
            if (all_scans_att.containsKey(aux2)) {
                new_rel2 = all_scans_att.get(aux2);
                temp_r = new_rel2;
                while (select_scan_coupled.containsKey(temp_r)) {
                    temp_r = select_scan_coupled.get(temp_r);
                }
            }

            if (joins.size() == 0) {
                Join new_j = new Join(temp, temp_r, sc);
                used_scans.add(new_rel);
                used_scans.add(new_rel2);
                joins.add(new_j);
            } else {
                Join new_j = null;
                for (Join j : joins) {
                    System.out.println(j.getPredicate());
                    if (!j.getPredicate().equals(sc)) {
                        if (!used_scans.contains(temp) && !used_scans.contains(temp_r)) {
                            used_scans.add(new_rel);
                            used_scans.add(new_rel2);
                            new_j = new Join(temp, temp_r, sc);
                        } else {
                            if (used_scans.contains(temp) && !used_scans.contains(temp_r)) {
                                used_scans.add(new_rel2);
                                new_j = new Join(j, temp_r, sc);
                            } else {
                                if (!used_scans.contains(temp) && used_scans.contains(temp_r)) {
                                    used_scans.add(new_rel);
                                    new_j = new Join(j, temp, sc);
                                }

                            }
                        }
                    }
                }
                if (new_j != null) {
                    if (joins.contains(new_j.getInputs().get(0))) {
                        joins.remove(new_j.getInputs().get(0));
                        joins.add(new_j);
                    } else {
                        joins.add(new_j);
                    }
                }
            }
        }
    }
    public void ProductCreation(){
        op_for_product.addAll(joins);
        for(Scan sc:all_scans_att.values()){
                if(!used_scans.contains(sc)){
                    op_for_product.add(sc);
                    used_scans.add(sc);
                }
        }
        if(op_for_product.size()>1) {
            for (int i = 0; i < op_for_product.size(); i++) {
                if (products.size() == 0) {
                    Product new_prod = new Product(op_for_product.get(i), op_for_product.get(i + 1));
                    products.add(new_prod);
                } else {
                    if(op_for_product.size() > i+1) {
                        System.out.println(products.get(products.size() - 1));
                        Product new_prod = new Product(products.get(products.size() - 1), op_for_product.get(i + 1));
                        products.add(new_prod);
                    }
                }
            }
        }
    }
}
